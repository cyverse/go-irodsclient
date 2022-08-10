package fs

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/connection"
	"github.com/cyverse/go-irodsclient/irods/message"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"

	log "github.com/sirupsen/logrus"
)

type TrackerCallBack func(processed int64, total int64)

// CloseDataObjectReplica closes a file handle of a data object replica, only used by parallel upload
func CloseDataObjectReplica(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) error {
	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	if !SupportParallUpload(conn) {
		// serial upload
		return fmt.Errorf("does not support close replica in current iRODS Version")
	}

	request := message.NewIRODSMessageClosereplicaRequest(handle.FileDescriptor, false, false, false, false)
	response := message.IRODSMessageClosereplicaResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
		return types.NewFileNotFoundErrorf("could not find a data object")
	}
	return err
}

// UploadDataObject put a data object at the local path to the iRODS path
func UploadDataObject(session *session.IRODSSession, localPath string, irodsPath string, resource string, replicate bool, callback TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObject",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return err
	}

	fileLength := stat.Size()

	logger.Debugf("upload data object - %s\n", localPath)

	conn, err := session.AcquireConnection()
	if err != nil {
		return err
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	f, err := os.OpenFile(localPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	// open a new file
	handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w", common.OPER_TYPE_PUT_DATA_OBJ)
	if err != nil {
		return err
	}

	totalBytesUploaded := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, fileLength)
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, err := f.Read(buffer)
		if bytesRead > 0 {
			err = WriteDataObject(conn, handle, buffer[:bytesRead])
			if err != nil {
				CloseDataObject(conn, handle)
				writeErr = err
				break
			}

			totalBytesUploaded += int64(bytesRead)
			if callback != nil {
				callback(totalBytesUploaded, fileLength)
			}
		}

		if err != nil {
			CloseDataObject(conn, handle)
			if err == io.EOF {
				break
			} else {
				writeErr = err
				break
			}
		}
	}

	var replErr error
	// replicate
	if replicate {
		err = ReplicateDataObject(conn, irodsPath, "", true, false)
		replErr = err
	}

	if writeErr != nil {
		return writeErr
	}

	return replErr
}

// SupportParallUpload checks if current server supports parallel upload
// available from 4.2.9
func SupportParallUpload(conn *connection.IRODSConnection) bool {
	irodsVersion := conn.GetVersion()
	return irodsVersion.HasHigherVersionThan(4, 2, 9)
}

// UploadDataObjectParallel put a data object at the local path to the iRODS path in parallel
// Partitions a file into n (taskNum) tasks and uploads in parallel
func UploadDataObjectParallel(session *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, callback TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectParallel",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return err
	}

	fileLength := stat.Size()

	conn, err := session.AcquireConnection()
	if err != nil {
		return err
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	if !SupportParallUpload(conn) {
		// serial upload
		return UploadDataObject(session, localPath, irodsPath, resource, replicate, callback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	logger.Debugf("upload data object in parallel - %s, size(%d), threads(%d)\n", irodsPath, fileLength, numTasks)

	// open a new file
	handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w", common.OPER_TYPE_PUT_DATA_OBJ)
	if err != nil {
		return err
	}

	replicaToken, resourceHierarchy, err := GetReplicaAccessInfo(conn, handle)
	if err != nil {
		return err
	}

	logger.Debugf("replicaToken %s, resourceHierarchy %s", replicaToken, resourceHierarchy)

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)

	uploadTask := func(taskOffset int64, taskLength int64) {
		defer taskWaitGroup.Done()

		taskConn, taskErr := session.AcquireConnection()
		if taskErr != nil {
			errChan <- taskErr
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		taskHandle, _, taskErr := OpenDataObjectWithReplicaToken(taskConn, irodsPath, resource, "a", replicaToken, resourceHierarchy)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer CloseDataObjectReplica(taskConn, taskHandle)

		f, taskErr := os.OpenFile(localPath, os.O_RDONLY, 0)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer f.Close()

		taskNewOffset, taskErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
		if taskErr != nil {
			errChan <- fmt.Errorf("could not seek a data object - %v", taskErr)
			return
		}

		if taskNewOffset != taskOffset {
			errChan <- fmt.Errorf("could not seek to target offset %d", taskOffset)
			return
		}

		taskRemain := taskLength

		// copy
		buffer := make([]byte, common.ReadWriteBufferSize)

		for taskRemain > 0 {
			bufferLen := common.ReadWriteBufferSize
			if taskRemain < int64(bufferLen) {
				bufferLen = int(taskRemain)
			}

			bytesRead, taskErr := f.ReadAt(buffer[:bufferLen], taskOffset+(taskLength-taskRemain))
			if bytesRead > 0 {
				taskErr = WriteDataObject(taskConn, taskHandle, buffer[:bytesRead])
				if taskErr != nil {
					errChan <- taskErr
					return
				}

				atomic.AddInt64(&totalBytesUploaded, int64(bytesRead))
				if callback != nil {
					callback(totalBytesUploaded, fileLength)
				}
			}

			taskRemain -= int64(bytesRead)

			if taskErr != nil {
				if taskErr == io.EOF {
					return
				}

				errChan <- taskErr
				return
			}
		}
	}

	lengthPerThread := fileLength / int64(numTasks)
	if fileLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, fileLength)
	}

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go uploadTask(offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	err = CloseDataObject(conn, handle)
	if err != nil {
		return err
	}

	// replicate
	if replicate {
		err = ReplicateDataObject(conn, irodsPath, "", true, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// UploadDataObjectParallelInBlockAsync put a data object at the local path to the iRODS path in parallel
// Chunks a file into fixed-size blocks and transfers them using n (taskNum) tasks in parallel
func UploadDataObjectParallelInBlockAsync(session *session.IRODSSession, localPath string, irodsPath string, resource string, blockLength int64, taskNum int, replicate bool) (chan int64, chan error) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectParallelInBlockAsync",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)
		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	fileLength := stat.Size()

	conn, err := session.AcquireConnection()
	if err != nil {
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)
		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)
		errChan <- fmt.Errorf("connection is nil or disconnected")
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if !SupportParallUpload(conn) {
		// serial upload
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)

		err := UploadDataObject(session, localPath, irodsPath, resource, replicate, nil)
		if err != nil {
			errChan <- err
			close(outputChan)
			close(errChan)
			return outputChan, errChan
		}

		outputChan <- 0
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	blockSize := blockLength
	if blockSize <= 0 {
		blockSize = util.GetBlockSizeForParallelTransfer(fileLength)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks == 1 {
		blockSize = fileLength
	}

	numBlocks := fileLength / blockSize
	if fileLength%blockSize != 0 {
		numBlocks++
	}

	logger.Debugf("upload data object in parallel - %s, size(%d), threads(%d), block_size(%d)\n", irodsPath, fileLength, numTasks, blockSize)

	inputChan := make(chan int64, numBlocks)
	outputChan := make(chan int64, numBlocks)
	errChan := make(chan error, numBlocks)

	// open a new file
	handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w", common.OPER_TYPE_PUT_DATA_OBJ)
	if err != nil {
		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	replicaToken, resourceHierarchy, err := GetReplicaAccessInfo(conn, handle)
	if err != nil {
		errChan <- err
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	offset := int64(0)
	for offset < fileLength {
		inputChan <- offset
		offset += blockSize
	}
	close(inputChan)

	taskWaitGroup := sync.WaitGroup{}

	uploadTask := func() {
		defer taskWaitGroup.Done()

		taskConn, taskErr := session.AcquireConnection()
		if taskErr != nil {
			errChan <- taskErr
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		// open the file with read-write mode
		taskHandle, _, taskErr := OpenDataObjectWithReplicaToken(taskConn, irodsPath, resource, "r+", replicaToken, resourceHierarchy)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer CloseDataObjectReplica(taskConn, taskHandle)

		f, taskErr := os.OpenFile(localPath, os.O_RDONLY, 0)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer f.Close()

		buffer := make([]byte, common.ReadWriteBufferSize)
		for {
			taskOffset, ok := <-inputChan
			if !ok {
				break
			}

			taskNewOffset, taskErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
			if taskErr != nil {
				errChan <- fmt.Errorf("could not seek a data object - %v", taskErr)
				return
			}

			if taskNewOffset != taskOffset {
				errChan <- fmt.Errorf("could not seek to target offset %d", taskOffset)
				return
			}

			taskRemain := blockSize

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, err := f.ReadAt(buffer[:bufferLen], taskOffset+(blockSize-taskRemain))
				if bytesRead > 0 {
					err = WriteDataObject(taskConn, taskHandle, buffer[:bytesRead])
					if err != nil {
						errChan <- err
						return
					}
				}

				taskRemain -= int64(bytesRead)

				if err != nil {
					if err == io.EOF {
						break
					}

					errChan <- err
					return
				}
			}

			// copy done
			outputChan <- taskOffset
		}
	}

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)
		go uploadTask()
	}

	go func() {
		// all tasks are done
		taskWaitGroup.Wait()

		err = CloseDataObject(conn, handle)
		if err != nil {
			errChan <- err
		} else {
			// replicate
			if replicate {
				err = ReplicateDataObject(conn, irodsPath, "", true, false)
				if err != nil {
					errChan <- err
				}
			}
		}

		close(outputChan)
		close(errChan)
	}()

	return outputChan, errChan
}

// DownloadDataObject downloads a data object at the iRODS path to the local path
func DownloadDataObject(session *session.IRODSSession, irodsPath string, resource string, localPath string, callback TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObject",
	})

	logger.Debugf("download data object - %s\n", irodsPath)

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	conn, err := session.AcquireConnection()
	if err != nil {
		return err
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, resource, "r")
	if err != nil {
		return err
	}
	defer CloseDataObject(conn, handle)

	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	totalBytesDownloaded := int64(0)

	buffer := make([]byte, common.ReadWriteBufferSize)
	// copy
	for {
		readLen, err := ReadDataObject(conn, handle, buffer)
		if err != nil && err != io.EOF {
			return err
		}

		_, err2 := f.Write(buffer[:readLen])
		if err2 != nil {
			return err2
		}

		totalBytesDownloaded += int64(readLen)
		if callback != nil {
			callback(totalBytesDownloaded, 0)
		}

		if err == io.EOF {
			return nil
		}
	}
}

// DownloadDataObjectParallel downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallel(session *session.IRODSSession, irodsPath string, resource string, localPath string, dataObjectLength int64, taskNum int, callback TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectParallel",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObjectLength)
	}

	logger.Debugf("download data object in parallel - %s, size(%d), threads(%d)\n", irodsPath, dataObjectLength, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)

	downloadTask := func(taskOffset int64, taskLength int64) {
		defer taskWaitGroup.Done()

		taskConn, taskErr := session.AcquireConnection()
		if taskErr != nil {
			errChan <- taskErr
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		taskHandle, _, taskErr := OpenDataObject(taskConn, irodsPath, resource, "r")
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer CloseDataObject(taskConn, taskHandle)

		f, taskErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer f.Close()

		taskNewOffset, taskErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
		if taskErr != nil {
			errChan <- fmt.Errorf("could not seek a data object - %v", taskErr)
			return
		}

		if taskNewOffset != taskOffset {
			errChan <- fmt.Errorf("could not seek to target offset %d", taskOffset)
			return
		}

		taskRemain := taskLength

		// copy
		buffer := make([]byte, common.ReadWriteBufferSize)
		for taskRemain > 0 {
			toCopy := taskRemain
			if toCopy >= int64(common.ReadWriteBufferSize) {
				toCopy = int64(common.ReadWriteBufferSize)
			}

			readLen, taskErr := ReadDataObject(taskConn, taskHandle, buffer[:toCopy])
			if readLen > 0 {
				_, taskErr2 := f.WriteAt(buffer[:readLen], taskOffset+(taskLength-taskRemain))
				if taskErr2 != nil {
					errChan <- taskErr2
					return
				}
				taskRemain -= int64(readLen)

				atomic.AddInt64(&totalBytesDownloaded, int64(readLen))
				if callback != nil {
					callback(totalBytesDownloaded, dataObjectLength)
				}
			}

			if taskErr != nil && taskErr != io.EOF {
				errChan <- taskErr
				return
			}

			if taskErr == io.EOF {
				// EOF
				return
			}
		}
	}

	lengthPerThread := dataObjectLength / int64(numTasks)
	if dataObjectLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, dataObjectLength)
	}

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// DownloadDataObjectParallelInBlocksAsync downloads a data object at the iRODS path to the local path in parallel
// Chunks a file into fixed-size blocks and transfers them using n (taskNum) tasks in parallel
func DownloadDataObjectParallelInBlocksAsync(session *session.IRODSSession, irodsPath string, resource string, localPath string, dataObjectLength int64, blockLength int64, taskNum int) (chan int64, chan error) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectParallelInBlocksAsync",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	blockSize := blockLength
	if blockSize <= 0 {
		blockSize = util.GetBlockSizeForParallelTransfer(dataObjectLength)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObjectLength)
	}

	if numTasks == 1 {
		blockSize = dataObjectLength
	}

	numBlocks := dataObjectLength / blockSize
	if dataObjectLength%blockSize != 0 {
		numBlocks++
	}

	logger.Debugf("download data object in parallel - %s, size(%d), threads(%d), block_size(%d)\n", irodsPath, dataObjectLength, numTasks, blockSize)

	inputChan := make(chan int64, numBlocks)
	outputChan := make(chan int64, numBlocks)
	errChan := make(chan error, numBlocks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		errChan <- err
		return outputChan, errChan
	}
	f.Close()

	offset := int64(0)
	for offset < dataObjectLength {
		inputChan <- offset
		offset += blockSize
	}
	close(inputChan)

	taskWaitGroup := sync.WaitGroup{}

	downloadTask := func() {
		defer taskWaitGroup.Done()

		taskConn, taskErr := session.AcquireConnection()
		if taskErr != nil {
			errChan <- taskErr
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		taskHandle, _, taskErr := OpenDataObject(taskConn, irodsPath, resource, "r")
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer CloseDataObject(taskConn, taskHandle)

		f, taskErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer f.Close()

		buffer := make([]byte, common.ReadWriteBufferSize)
		for {
			taskOffset, ok := <-inputChan
			if !ok {
				break
			}

			taskNewOffset, readErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
			if readErr != nil {
				errChan <- fmt.Errorf("could not seek a data object - %v", readErr)
				return
			}

			if taskNewOffset != taskOffset {
				errChan <- fmt.Errorf("could not seek to target offset %d", taskOffset)
				return
			}

			taskRemain := blockSize

			// copy
			for taskRemain > 0 {
				toCopy := taskRemain
				if toCopy >= int64(common.ReadWriteBufferSize) {
					toCopy = int64(common.ReadWriteBufferSize)
				}

				readLen, readErr := ReadDataObject(taskConn, taskHandle, buffer[:toCopy])
				if readLen > 0 {
					_, readErr2 := f.WriteAt(buffer[:readLen], taskOffset+(blockSize-taskRemain))
					if readErr2 != nil {
						errChan <- readErr2
						return
					}
					taskRemain -= int64(readLen)
				}

				if readErr != nil && readErr != io.EOF {
					errChan <- readErr
					return
				}

				if readErr == io.EOF {
					// EOF
					return
				}
			}

			// copy done
			outputChan <- taskOffset
		}
	}

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)
		go downloadTask()
	}

	go func() {
		// all tasks are done
		taskWaitGroup.Wait()

		close(outputChan)
		close(errChan)
	}()

	return outputChan, errChan
}
