package fs

import (
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
	"golang.org/x/xerrors"

	log "github.com/sirupsen/logrus"
)

// CloseDataObjectReplica closes a file handle of a data object replica, only used by parallel upload
func CloseDataObjectReplica(conn *connection.IRODSConnection, handle *types.IRODSFileHandle) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	if !conn.SupportParallelUpload() {
		// serial upload
		return xerrors.Errorf("does not support close replica in current iRODS Version")
	}

	request := message.NewIRODSMessageCloseDataObjectReplicaRequest(handle.FileDescriptor, false, false, false, false)
	response := message.IRODSMessageCloseDataObjectReplicaResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND {
			return types.NewFileNotFoundErrorf("failed to find a data object")
		}
		return xerrors.Errorf("failed to close data object replica: %w", err)
	}
	return nil
}

// UploadDataObject put a data object at the local path to the iRODS path
func UploadDataObject(session *session.IRODSSession, localPath string, irodsPath string, resource string, replicate bool, callback common.TrackerCallBack) error {
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
		return xerrors.Errorf("failed to stat file %s: %w", localPath, err)
	}

	fileLength := stat.Size()

	logger.Debugf("upload data object - %s\n", localPath)

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	f, err := os.OpenFile(localPath, os.O_RDONLY, 0)
	if err != nil {
		return xerrors.Errorf("failed to open file %s: %w", localPath, err)
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

	// block write call-back
	var blockWriteCallback common.TrackerCallBack
	if callback != nil {
		blockWriteCallback = func(processed int64, total int64) {
			callback(totalBytesUploaded+processed, fileLength)
		}
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var returnErr error
	for {
		bytesRead, readErr := f.Read(buffer)
		if bytesRead > 0 {
			writeErr := WriteDataObjectWithTrackerCallBack(conn, handle, buffer[:bytesRead], blockWriteCallback)
			if writeErr != nil {
				CloseDataObject(conn, handle)
				returnErr = writeErr
				break
			}

			totalBytesUploaded += int64(bytesRead)
			if callback != nil {
				callback(totalBytesUploaded, fileLength)
			}
		}

		if readErr != nil {
			CloseDataObject(conn, handle)
			if readErr == io.EOF {
				break
			} else {
				returnErr = xerrors.Errorf("failed to read from file %s: %w", localPath, readErr)
				break
			}
		}
	}

	var replErr error
	// replicate
	if replicate {
		replErr = ReplicateDataObject(conn, irodsPath, "", true, false)
	}

	if returnErr != nil {
		return returnErr
	}

	return replErr
}

// UploadDataObjectParallel put a data object at the local path to the iRODS path in parallel
// Partitions a file into n (taskNum) tasks and uploads in parallel
func UploadDataObjectParallel(session *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectParallel",
	})

	if !session.SupportParallelUpload() {
		// serial upload
		return UploadDataObject(session, localPath, irodsPath, resource, replicate, callback)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %s: %w", localPath, err)
	}

	fileLength := stat.Size()

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
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

	// block write call-back
	var blockWriteCallback common.TrackerCallBack
	if callback != nil {
		blockWriteCallback = func(processed int64, total int64) {
			callback(totalBytesUploaded+processed, fileLength)
		}
	}

	uploadTask := func(taskOffset int64, taskLength int64) {
		defer taskWaitGroup.Done()

		taskConn, taskErr := session.AcquireConnection()
		if taskErr != nil {
			errChan <- xerrors.Errorf("failed to get connection: %w", taskErr)
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
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
			errChan <- xerrors.Errorf("failed to open file %s: %w", localPath, taskErr)
			return
		}
		defer f.Close()

		taskNewOffset, taskErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
		if taskErr != nil {
			errChan <- taskErr
			return
		}

		if taskNewOffset != taskOffset {
			errChan <- xerrors.Errorf("failed to seek to target offset %d", taskOffset)
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

			bytesRead, taskReadErr := f.ReadAt(buffer[:bufferLen], taskOffset+(taskLength-taskRemain))
			if bytesRead > 0 {
				taskWriteErr := WriteDataObjectWithTrackerCallBack(taskConn, taskHandle, buffer[:bytesRead], blockWriteCallback)
				if taskWriteErr != nil {
					errChan <- taskWriteErr
					return
				}

				atomic.AddInt64(&totalBytesUploaded, int64(bytesRead))
				if callback != nil {
					callback(totalBytesUploaded, fileLength)
				}
			}

			taskRemain -= int64(bytesRead)

			if taskReadErr != nil {
				if taskReadErr == io.EOF {
					return
				}

				errChan <- xerrors.Errorf("failed to read from file %s: %w", localPath, taskReadErr)
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
		errChan <- xerrors.Errorf("failed to stat file %s: %w", localPath, err)
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	fileLength := stat.Size()

	conn, err := session.AcquireConnection()
	if err != nil {
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)
		errChan <- xerrors.Errorf("failed to get connection: %w", err)
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		outputChan := make(chan int64, 1)
		errChan := make(chan error, 1)
		errChan <- xerrors.Errorf("connection is nil or disconnected")
		close(outputChan)
		close(errChan)
		return outputChan, errChan
	}

	if !conn.SupportParallelUpload() {
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
			errChan <- xerrors.Errorf("failed to get connection: %w", err)
		}
		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
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
			errChan <- xerrors.Errorf("failed to open file %s: %w", localPath, taskErr)
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
				errChan <- taskErr
				return
			}

			if taskNewOffset != taskOffset {
				errChan <- xerrors.Errorf("failed to seek to target offset %d", taskOffset)
				return
			}

			taskRemain := blockSize

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, taskReadErr := f.ReadAt(buffer[:bufferLen], taskOffset+(blockSize-taskRemain))
				if bytesRead > 0 {
					taskWriteErr := WriteDataObject(taskConn, taskHandle, buffer[:bytesRead])
					if taskWriteErr != nil {
						errChan <- taskWriteErr
						return
					}
				}

				taskRemain -= int64(bytesRead)

				if taskReadErr != nil {
					if taskReadErr == io.EOF {
						break
					}

					errChan <- xerrors.Errorf("failed to read from file %s: %w", localPath, taskReadErr)
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
func DownloadDataObject(session *session.IRODSSession, irodsPath string, resource string, localPath string, dataObjectLength int64, callback common.TrackerCallBack) error {
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
		return xerrors.Errorf("failed top get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, resource, "r")
	if err != nil {
		return err
	}
	defer CloseDataObject(conn, handle)

	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed top create file %s: %w", localPath, err)
	}
	defer f.Close()

	totalBytesDownloaded := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, dataObjectLength)
	}

	// block read call-back
	var blockReadCallback common.TrackerCallBack
	if callback != nil {
		blockReadCallback = func(processed int64, total int64) {
			callback(totalBytesDownloaded+processed, dataObjectLength)
		}
	}

	buffer := make([]byte, common.ReadWriteBufferSize)
	// copy
	for {
		readLen, readErr := ReadDataObjectWithTrackerCallBack(conn, handle, buffer, blockReadCallback)
		if readErr != nil && readErr != io.EOF {
			return readErr
		}

		_, writeErr := f.Write(buffer[:readLen])
		if writeErr != nil {
			return xerrors.Errorf("failed to write to file %s: %w", localPath, writeErr)
		}

		totalBytesDownloaded += int64(readLen)
		if callback != nil {
			callback(totalBytesDownloaded, dataObjectLength)
		}

		if readErr == io.EOF {
			return nil
		}
	}
}

// DownloadDataObjectParallel downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallel(session *session.IRODSSession, irodsPath string, resource string, localPath string, dataObjectLength int64, taskNum int, callback common.TrackerCallBack) error {
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

	if numTasks > session.GetConfig().ConnectionMax {
		numTasks = session.GetConfig().ConnectionMax
	}

	logger.Debugf("download data object in parallel - %s, size(%d), threads(%d)\n", irodsPath, dataObjectLength, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %s: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)

	// block reads
	blockReads := make([]int64, numTasks)

	// get connections
	connections, err := session.AcquireConnectionsMulti(numTasks)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	downloadTask := func(taskID int, taskConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		blockReads[taskID] = 0

		defer taskWaitGroup.Done()

		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
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
			errChan <- xerrors.Errorf("failed to open file %s: %w", localPath, taskErr)
			return
		}
		defer f.Close()

		taskNewOffset, taskErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
		if taskErr != nil {
			errChan <- taskErr
			return
		}

		if taskNewOffset != taskOffset {
			errChan <- xerrors.Errorf("failed to seek to target offset %d", taskOffset)
			return
		}

		var blockReadCallback common.TrackerCallBack
		if callback != nil {
			blockReadCallback = func(processed int64, total int64) {
				if processed > 0 {
					delta := processed - blockReads[taskID]
					blockReads[taskID] = processed

					atomic.AddInt64(&totalBytesDownloaded, int64(delta))
					callback(totalBytesDownloaded, dataObjectLength)
				}
			}
		}

		taskRemain := taskLength

		// copy
		buffer := make([]byte, common.ReadWriteBufferSize)
		for taskRemain > 0 {
			toCopy := taskRemain
			if toCopy >= int64(common.ReadWriteBufferSize) {
				toCopy = int64(common.ReadWriteBufferSize)
			}

			blockReads[taskID] = 0
			readLen, readTaskErr := ReadDataObjectWithTrackerCallBack(taskConn, taskHandle, buffer[:toCopy], blockReadCallback)
			if readLen > 0 {
				_, writeTaskErr := f.WriteAt(buffer[:readLen], taskOffset+(taskLength-taskRemain))
				if writeTaskErr != nil {
					errChan <- xerrors.Errorf("failed to write to file %s: %w", localPath, writeTaskErr)
					return
				}
				taskRemain -= int64(readLen)
			}

			if readTaskErr != nil && readTaskErr != io.EOF {
				errChan <- readTaskErr
				return
			}

			if readTaskErr == io.EOF {
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

		go downloadTask(i, connections[i], offset, lengthPerThread)
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

	if numTasks > session.GetConfig().ConnectionMax {
		numTasks = session.GetConfig().ConnectionMax
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
		errChan <- xerrors.Errorf("failed to create file %s: %w", localPath, err)
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

	// get connections
	connections, err := session.AcquireConnectionsMulti(numTasks)
	if err != nil {
		errChan <- xerrors.Errorf("failed to get connection: %w", err)
		return outputChan, errChan
	}

	downloadTask := func(taskConn *connection.IRODSConnection) {
		defer taskWaitGroup.Done()

		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
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
			errChan <- xerrors.Errorf("failed to open file %s: %w", localPath, taskErr)
			return
		}
		defer f.Close()

		buffer := make([]byte, common.ReadWriteBufferSize)
		for {
			taskOffset, ok := <-inputChan
			if !ok {
				break
			}

			taskNewOffset, seekErr := SeekDataObject(taskConn, taskHandle, taskOffset, types.SeekSet)
			if seekErr != nil {
				errChan <- seekErr
				return
			}

			if taskNewOffset != taskOffset {
				errChan <- xerrors.Errorf("failed to seek to target offset %d", taskOffset)
				return
			}

			taskRemain := blockSize

			// copy
			for taskRemain > 0 {
				toCopy := taskRemain
				if toCopy >= int64(common.ReadWriteBufferSize) {
					toCopy = int64(common.ReadWriteBufferSize)
				}

				readLen, readTaskErr := ReadDataObject(taskConn, taskHandle, buffer[:toCopy])
				if readLen > 0 {
					_, writeTaskErr := f.WriteAt(buffer[:readLen], taskOffset+(blockSize-taskRemain))
					if writeTaskErr != nil {
						errChan <- xerrors.Errorf("failed to write to file %s: %w", localPath, writeTaskErr)
						return
					}
					taskRemain -= int64(readLen)
				}

				if readTaskErr != nil && readTaskErr != io.EOF {
					errChan <- readTaskErr
					return
				}

				if readTaskErr == io.EOF {
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
		go downloadTask(connections[i])
	}

	go func() {
		// all tasks are done
		taskWaitGroup.Wait()

		close(outputChan)
		close(errChan)
	}()

	return outputChan, errChan
}
