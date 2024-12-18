package fs

import (
	"bytes"
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

	request := message.NewIRODSMessageCloseDataObjectReplicaRequest(handle.FileDescriptor, false, false, false, false, false)
	response := message.IRODSMessageCloseDataObjectReplicaResponse{}
	err := conn.RequestAndCheck(request, &response, nil)
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to find the data object for path %q: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return xerrors.Errorf("failed to find the collection for path %q: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}

		return xerrors.Errorf("failed to close data object replica: %w", err)
	}
	return nil
}

// UploadDataObjectFromBuffer put a data object to the iRODS path from buffer
func UploadDataObjectFromBuffer(session *session.IRODSSession, buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	fileLength := int64(buffer.Len())

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// open a new file
	handle, err := CreateDataObject(conn, irodsPath, resource, "w+", true, keywords)
	//handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}

	totalBytesUploaded := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, fileLength)
	}

	// copy
	writeErr := WriteDataObjectWithTrackerCallBack(conn, handle, buffer.Bytes(), nil)
	if callback != nil {
		callback(fileLength, fileLength)
	}

	CloseDataObject(conn, handle)

	if writeErr != nil {
		return writeErr
	}

	// replicate
	if replicate {
		replErr := ReplicateDataObject(conn, irodsPath, "", true, false)
		if replErr != nil {
			return replErr
		}
	}

	return nil
}

// UploadDataObject put a data object at the local path to the iRODS path
func UploadDataObject(session *session.IRODSSession, localPath string, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
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
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	logger.Debugf("upload data object %q", localPath)

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
		return xerrors.Errorf("failed to open file %q: %w", localPath, err)
	}
	defer f.Close()

	// open a new file
	handle, err := CreateDataObject(conn, irodsPath, resource, "w+", true, keywords)
	//handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}

	totalBytesUploaded := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, fileLength)
	}

	// block write call-back
	blockWriteCallback := func(processed int64, total int64) {
		if callback != nil {
			callback(totalBytesUploaded+processed, fileLength)
		}
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, readErr := f.Read(buffer)
		if bytesRead > 0 {
			writeErr = WriteDataObjectWithTrackerCallBack(conn, handle, buffer[:bytesRead], blockWriteCallback)
			if writeErr != nil {
				break
			}

			totalBytesUploaded += int64(bytesRead)
			if callback != nil {
				callback(totalBytesUploaded, fileLength)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				writeErr = xerrors.Errorf("failed to read file %q: %w", localPath, readErr)
				break
			}
		}
	}

	CloseDataObject(conn, handle)

	if writeErr != nil {
		return writeErr
	}

	// replicate
	if replicate {
		replErr := ReplicateDataObject(conn, irodsPath, "", true, false)
		if replErr != nil {
			return replErr
		}
	}

	return nil
}

// UploadDataObjectParallel put a data object at the local path to the iRODS path in parallel
// Partitions a file into n (taskNum) tasks and uploads in parallel
func UploadDataObjectParallel(session *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectParallel",
	})

	if !session.SupportParallelUpload() {
		// serial upload
		return UploadDataObject(session, localPath, irodsPath, resource, replicate, keywords, callback)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks == 1 {
		// serial upload
		return UploadDataObject(session, localPath, irodsPath, resource, replicate, keywords, callback)
	}

	conn, err := session.AcquireUnmanagedConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.DiscardConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	logger.Debugf("upload data object in parallel %s, size(%d), threads(%d)", irodsPath, fileLength, numTasks)

	// open a new file
	handle, err := OpenDataObjectForPutParallel(conn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, numTasks, fileLength, keywords)
	if err != nil {
		return err
	}

	replicaToken, resourceHierarchy, err := GetReplicaAccessInfo(conn, handle)
	if err != nil {
		CloseDataObject(conn, handle)
		return err
	}

	logger.Debugf("replicaToken %s, resourceHierarchy %s", replicaToken, resourceHierarchy)

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, fileLength)
	}

	uploadTask := func(taskOffset int64, taskLength int64) {
		defer taskWaitGroup.Done()

		// we will not reuse connection from the pool, as it should use fresh one
		taskConn, taskErr := session.AcquireUnmanagedConnection()
		if taskErr != nil {
			errChan <- xerrors.Errorf("failed to get connection: %w", taskErr)
			return
		}
		defer session.DiscardConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
			return
		}

		// open the file with read-write mode
		// to not seek to end
		taskHandle, _, taskErr := OpenDataObjectWithReplicaToken(taskConn, irodsPath, resource, "w", replicaToken, resourceHierarchy, numTasks, fileLength, keywords)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer func() {
			errClose := CloseDataObjectReplica(taskConn, taskHandle)
			if errClose != nil {
				errChan <- errClose
			}
		}()

		f, taskErr := os.OpenFile(localPath, os.O_RDONLY, 0)
		if taskErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, taskErr)
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
		var taskWriteErr error
		for taskRemain > 0 {
			bufferLen := common.ReadWriteBufferSize
			if taskRemain < int64(bufferLen) {
				bufferLen = int(taskRemain)
			}

			bytesRead, taskReadErr := f.ReadAt(buffer[:bufferLen], taskOffset+(taskLength-taskRemain))
			if bytesRead > 0 {
				taskWriteErr = WriteDataObjectWithTrackerCallBack(taskConn, taskHandle, buffer[:bytesRead], nil)
				if taskWriteErr != nil {
					break
				}

				atomic.AddInt64(&totalBytesUploaded, int64(bytesRead))
				if callback != nil {
					callback(totalBytesUploaded, fileLength)
				}

				taskRemain -= int64(bytesRead)
			}

			if taskReadErr != nil {
				if taskReadErr == io.EOF {
					break
				} else {
					taskWriteErr = xerrors.Errorf("failed to read file %q: %w", localPath, taskReadErr)
					break
				}
			}
		}

		if taskWriteErr != nil {
			errChan <- taskWriteErr
		}
	}

	lengthPerThread := fileLength / int64(numTasks)
	if fileLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go uploadTask(offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		CloseDataObject(conn, handle)
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

// DownloadDataObjectToBuffer downloads a data object at the iRODS path to buffer
func DownloadDataObjectToBuffer(session *session.IRODSSession, irodsPath string, resource string, buffer *bytes.Buffer, dataObjectLength int64, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObject",
	})

	logger.Debugf("download data object %q", irodsPath)

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, resource, "r", keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}
	defer CloseDataObject(conn, handle)

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

	buffer2 := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	// copy
	for {
		bytesRead, readErr := ReadDataObjectWithTrackerCallBack(conn, handle, buffer2, blockReadCallback)
		if bytesRead > 0 {
			_, writeErr = buffer.Write(buffer2[:bytesRead])
			if writeErr != nil {
				break
			}

			totalBytesDownloaded += int64(bytesRead)
			if callback != nil {
				callback(totalBytesDownloaded, dataObjectLength)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				return xerrors.Errorf("failed to read data object %q: %w", irodsPath, readErr)
			}
		}
	}

	if writeErr != nil {
		return writeErr
	}

	return nil
}

// DownloadDataObject downloads a data object at the iRODS path to the local path
func DownloadDataObject(session *session.IRODSSession, irodsPath string, resource string, localPath string, fileLength int64, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObject",
	})

	logger.Debugf("download data object %q", irodsPath)

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, resource, "r", keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}
	defer CloseDataObject(conn, handle)

	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	defer f.Close()

	totalBytesDownloaded := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, fileLength)
	}

	// block read call-back
	blockReadCallback := func(processed int64, total int64) {
		if callback != nil {
			callback(totalBytesDownloaded+processed, fileLength)
		}
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, readErr := ReadDataObjectWithTrackerCallBack(conn, handle, buffer, blockReadCallback)
		if bytesRead > 0 {
			_, writeErr = f.Write(buffer[:bytesRead])
			if writeErr != nil {
				break
			}

			totalBytesDownloaded += int64(bytesRead)
			if callback != nil {
				callback(totalBytesDownloaded, fileLength)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				writeErr = xerrors.Errorf("failed to read data object %q: %w", irodsPath, readErr)
				break
			}
		}
	}

	if writeErr != nil {
		return writeErr
	}

	return nil
}

// DownloadDataObjectResumable downloads a data object at the iRODS path to the local path with support of transfer resume
func DownloadDataObjectResumable(session *session.IRODSSession, irodsPath string, resource string, localPath string, fileLength int64, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectResumable",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	logger.Debugf("download data object %q", irodsPath)

	// create transfer status
	transferStatusLocal, err := GetOrNewDataObjectTransferStatusLocal(localPath, fileLength, 1)
	if err != nil {
		return xerrors.Errorf("failed to read transfer status file for %q: %w", localPath, err)
	}

	err = transferStatusLocal.CreateStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to create transfer status file for %q: %w", localPath, err)
	}

	err = transferStatusLocal.WriteHeader()
	if err != nil {
		transferStatusLocal.CloseStatusFile()
		return xerrors.Errorf("failed to write transfer status file header for %q: %w", localPath, err)
	}

	conn, err := session.AcquireConnection()
	if err != nil {
		transferStatusLocal.CloseStatusFile()
		return xerrors.Errorf("failed to get connection: %w", err)
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		transferStatusLocal.CloseStatusFile() //nolint
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, irodsPath, resource, "r", keywords)
	if err != nil {
		transferStatusLocal.CloseStatusFile() //nolint
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}

	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		transferStatusLocal.CloseStatusFile() //nolint
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	defer f.Close()

	// seek to last failure point
	transferStatus := transferStatusLocal.GetStatus()
	lastOffset := int64(0)
	if transferStatus != nil {
		if transferStatusEntry, ok := transferStatus.StatusMap[0]; ok {
			lastOffset = transferStatusEntry.StartOffset + transferStatusEntry.CompletedLength
		}
	}

	if lastOffset > 0 {
		logger.Debugf("resuming downloading data object %q from offset %d", irodsPath, lastOffset)

		newOffset, err := SeekDataObject(conn, handle, lastOffset, types.SeekSet)
		if err != nil {
			transferStatusLocal.CloseStatusFile() //nolint
			return xerrors.Errorf("failed to seek data object %q to offset %d: %w", irodsPath, lastOffset, err)
		}

		offset, err := f.Seek(lastOffset, io.SeekStart)
		if err != nil {
			transferStatusLocal.CloseStatusFile() //nolint
			return xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, err)
		}

		if newOffset != offset {
			transferStatusLocal.CloseStatusFile() //nolint
			return xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
		}
	}

	totalBytesDownloaded := lastOffset
	if callback != nil {
		if lastOffset > 0 {
			callback(0, fileLength)
		}
		callback(lastOffset, fileLength)
	}

	// block read call-back
	blockReadCallback := func(processed int64, total int64) {
		if callback != nil {
			callback(totalBytesDownloaded+processed, fileLength)
		}
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, readErr := ReadDataObjectWithTrackerCallBack(conn, handle, buffer, blockReadCallback)
		if bytesRead > 0 {
			_, writeErr = f.Write(buffer[:bytesRead])
			if writeErr != nil {
				break
			}

			totalBytesDownloaded += int64(bytesRead)

			// write status
			transferStatusEntry := &DataObjectTransferStatusEntry{
				StartOffset:     0,
				Length:          fileLength,
				CompletedLength: totalBytesDownloaded,
			}
			transferStatusLocal.WriteStatus(transferStatusEntry) //nolint

			if callback != nil {
				callback(totalBytesDownloaded, fileLength)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				writeErr = xerrors.Errorf("failed to read from data object %q: %w", irodsPath, readErr)
				break
			}
		}
	}

	err = transferStatusLocal.CloseStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to close status file: %w", err)
	}

	err = CloseDataObject(conn, handle)
	if err != nil {
		return xerrors.Errorf("failed to close data object: %w", err)
	}

	if writeErr != nil {
		return writeErr
	}

	err = transferStatusLocal.DeleteStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to delete status file: %w", err)
	}

	return nil
}

// DownloadDataObjectParallel downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallel(session *session.IRODSSession, irodsPath string, resource string, localPath string, fileLength int64, taskNum int, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
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
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks > session.GetConfig().ConnectionMaxNumber {
		numTasks = session.GetConfig().ConnectionMaxNumber
	}

	if numTasks == 1 {
		// serial download
		return DownloadDataObject(session, irodsPath, resource, localPath, fileLength, keywords, callback)
	}

	logger.Debugf("download data object in parallel %s, size(%d), threads(%d)", irodsPath, fileLength, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, fileLength)
	}

	// task progress
	taskProgress := make([]int64, numTasks)

	// get connections
	connections, err := session.AcquireConnectionsMulti(numTasks)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	downloadTask := func(taskID int, taskOffset int64, taskLength int64) {
		taskProgress[taskID] = 0
		taskConn := connections[taskID]

		defer taskWaitGroup.Done()

		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
			return
		}

		taskHandle, _, taskErr := OpenDataObject(taskConn, irodsPath, resource, "r", keywords)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer func() {
			errClose := CloseDataObject(taskConn, taskHandle)
			if errClose != nil {
				errChan <- errClose
			}
		}()

		f, taskErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if taskErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, taskErr)
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

		blockReadCallback := func(processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				if callback != nil {
					callback(totalBytesDownloaded+delta, fileLength)
				}
			}
		}

		taskRemain := taskLength

		// copy
		buffer := make([]byte, common.ReadWriteBufferSize)
		var taskWriteErr error
		for taskRemain > 0 {
			bufferLen := common.ReadWriteBufferSize
			if taskRemain < int64(bufferLen) {
				bufferLen = int(taskRemain)
			}

			taskProgress[taskID] = 0

			bytesRead, taskReadErr := ReadDataObjectWithTrackerCallBack(taskConn, taskHandle, buffer[:bufferLen], blockReadCallback)
			if bytesRead > 0 {
				_, taskWriteErr = f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
				if taskWriteErr != nil {
					break
				}

				atomic.AddInt64(&totalBytesDownloaded, int64(bytesRead))

				taskRemain -= int64(bytesRead)
			}

			if taskReadErr != nil {
				if taskReadErr == io.EOF {
					break
				} else {
					taskWriteErr = xerrors.Errorf("failed to read data object %q: %w", irodsPath, taskReadErr)
				}
			}
		}

		if taskWriteErr != nil {
			errChan <- taskWriteErr
		}
	}

	lengthPerThread := fileLength / int64(numTasks)
	if fileLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// DownloadDataObjectParallelResumable downloads a data object at the iRODS path to the local path in parallel with support of transfer resume
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallelResumable(session *session.IRODSSession, irodsPath string, resource string, localPath string, fileLength int64, taskNum int, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectParallelResumable",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks > session.GetConfig().ConnectionMaxNumber {
		numTasks = session.GetConfig().ConnectionMaxNumber
	}

	if numTasks == 1 {
		// serial download
		return DownloadDataObjectResumable(session, irodsPath, resource, localPath, fileLength, keywords, callback)
	}

	logger.Debugf("downloading data object in parallel %s, size(%d)", irodsPath, fileLength)

	// create transfer status
	transferStatusLocal, err := GetOrNewDataObjectTransferStatusLocal(localPath, fileLength, numTasks)
	if err != nil {
		return xerrors.Errorf("failed to read transfer status file for %q: %w", localPath, err)
	}

	// if previous transfer used different number of threads, use old value
	numTasks = transferStatusLocal.status.Threads

	logger.Debugf("use %d tasks to download", numTasks)

	if numTasks == 1 {
		// serial download
		return DownloadDataObjectResumable(session, irodsPath, resource, localPath, fileLength, keywords, callback)
	}

	err = transferStatusLocal.CreateStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to create transfer status file for %q: %w", localPath, err)
	}

	err = transferStatusLocal.WriteHeader()
	if err != nil {
		transferStatusLocal.CloseStatusFile() //nolint
		return xerrors.Errorf("failed to write transfer status file header for %q: %w", localPath, err)
	}

	// create an empty file
	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, fileLength)
	}

	// task progress
	taskProgress := make([]int64, numTasks)

	// get connections
	connections, err := session.AcquireConnectionsMulti(numTasks)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	downloadTask := func(taskID int, taskOffset int64, taskLength int64) {
		taskProgress[taskID] = 0
		taskConn := connections[taskID]

		defer taskWaitGroup.Done()

		defer session.ReturnConnection(taskConn)

		if taskConn == nil || !taskConn.IsConnected() {
			errChan <- xerrors.Errorf("connection is nil or disconnected")
			return
		}

		taskHandle, _, taskErr := OpenDataObject(taskConn, irodsPath, resource, "r", keywords)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer func() {
			errClose := CloseDataObject(taskConn, taskHandle)
			if errClose != nil {
				errChan <- errClose
			}
		}()

		f, taskErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if taskErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, taskErr)
			return
		}
		defer f.Close()

		// seek to last failure point
		transferStatus := transferStatusLocal.GetStatus()
		lastOffset := int64(taskOffset)
		if transferStatus != nil {
			if transferStatusEntry, ok := transferStatus.StatusMap[taskOffset]; ok {
				lastOffset = transferStatusEntry.StartOffset + transferStatusEntry.CompletedLength
			}
		}

		if lastOffset > 0 {
			logger.Debugf("resuming downloading data object %q for task offset %d from offset %d", irodsPath, taskOffset, lastOffset)

			newOffset, err := SeekDataObject(taskConn, taskHandle, lastOffset, types.SeekSet)
			if err != nil {
				errChan <- xerrors.Errorf("failed to seek data object %q to offset %d: %w", irodsPath, lastOffset, err)
				return
			}

			taskNewOffset, taskErr := f.Seek(lastOffset, io.SeekStart)
			if taskErr != nil {
				errChan <- xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, err)
				return
			}

			if newOffset != taskNewOffset {
				errChan <- xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
				return
			}
		}

		blockReadCallback := func(processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				if callback != nil {
					callback(totalBytesDownloaded+delta, fileLength)
				}
			}
		}

		taskRemain := taskLength - (lastOffset - taskOffset)
		if lastOffset-taskOffset > 0 {
			// increase counter
			atomic.AddInt64(&totalBytesDownloaded, lastOffset-taskOffset)
			if callback != nil {
				callback(totalBytesDownloaded, fileLength)
			}
		}

		// copy
		buffer := make([]byte, common.ReadWriteBufferSize)
		var taskWriteErr error
		for taskRemain > 0 {
			bufferLen := common.ReadWriteBufferSize
			if taskRemain < int64(bufferLen) {
				bufferLen = int(taskRemain)
			}

			taskProgress[taskID] = 0

			bytesRead, taskReadErr := ReadDataObjectWithTrackerCallBack(taskConn, taskHandle, buffer[:bufferLen], blockReadCallback)
			if bytesRead > 0 {
				_, taskWriteErr = f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
				if taskWriteErr != nil {
					break
				}

				atomic.AddInt64(&totalBytesDownloaded, int64(bytesRead))

				// write status
				transferStatusEntry := &DataObjectTransferStatusEntry{
					StartOffset:     taskOffset,
					Length:          taskLength,
					CompletedLength: (taskLength - taskRemain) + int64(bytesRead),
				}
				transferStatusLocal.WriteStatus(transferStatusEntry) //nolint

				if callback != nil {
					callback(totalBytesDownloaded, fileLength)
				}

				taskRemain -= int64(bytesRead)
			}

			if taskReadErr != nil {
				if taskReadErr == io.EOF {
					break
				} else {
					taskWriteErr = xerrors.Errorf("failed to read from file %q: %w", irodsPath, taskReadErr)
				}
			}
		}

		if taskWriteErr != nil {
			errChan <- taskWriteErr
		}
	}

	lengthPerThread := fileLength / int64(numTasks)
	if fileLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		transferStatusLocal.CloseStatusFile()
		return <-errChan
	}

	err = transferStatusLocal.CloseStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to close status file: %w", err)
	}

	err = transferStatusLocal.DeleteStatusFile()
	if err != nil {
		return xerrors.Errorf("failed to delete status file: %w", err)
	}

	return nil
}
