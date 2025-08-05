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
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
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
func UploadDataObjectFromBuffer(sess *session.IRODSSession, buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	fileLength := int64(buffer.Len())

	conn, err := sess.AcquireConnection(false)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	defer sess.ReturnConnection(conn)

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
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// copy
	writeErr := WriteDataObjectWithTrackerCallBack(conn, handle, buffer.Bytes(), nil)
	if transferCallback != nil {
		transferCallback("upload", fileLength, fileLength)
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

// UploadDataObjectFromBufferWithConnections put a data object to the iRODS path from buffer
func UploadDataObjectFromBufferWithConnection(conn *connection.IRODSConnection, buffer *bytes.Buffer, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	fileLength := int64(buffer.Len())

	// open a new file
	handle, err := CreateDataObject(conn, irodsPath, resource, "w+", true, keywords)
	//handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", irodsPath, err)
	}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// copy
	writeErr := WriteDataObjectWithTrackerCallBack(conn, handle, buffer.Bytes(), nil)
	if transferCallback != nil {
		transferCallback("upload", fileLength, fileLength)
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
func UploadDataObject(sess *session.IRODSSession, localPath string, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"local_path": localPath,
		"irods_path": irodsPath,
		"resource":   resource,
		"replicate":  replicate,
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	logger.Debug("upload data object")

	conn, err := sess.AcquireConnection(false)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	defer sess.ReturnConnection(conn)

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
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// block write call-back
	blockWriteCallback := func(taskName string, processed int64, total int64) {
		if transferCallback != nil {
			transferCallback("upload", totalBytesUploaded+processed, fileLength)
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
			if transferCallback != nil {
				transferCallback("upload", totalBytesUploaded, fileLength)
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

// UploadDataObjectWithConnection put a data object at the local path to the iRODS path
func UploadDataObjectWithConnection(conn *connection.IRODSConnection, localPath string, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"local_path": localPath,
		"irods_path": irodsPath,
		"resource":   resource,
		"replicate":  replicate,
	})

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	logger.Debug("upload data object")

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
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// block write call-back
	blockWriteCallback := func(taskName string, processed int64, total int64) {
		if transferCallback != nil {
			transferCallback("upload", totalBytesUploaded+processed, fileLength)
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
			if transferCallback != nil {
				transferCallback("upload", totalBytesUploaded, fileLength)
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
func UploadDataObjectParallel(sess *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"local_path": localPath,
		"irods_path": irodsPath,
		"resource":   resource,
		"task_num":   taskNum,
		"replicate":  replicate,
	})

	if !sess.SupportParallelUpload() {
		// serial upload
		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	if fileLength == 0 {
		// empty file
		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks == 1 {
		// serial upload
		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	// acquire all connections
	// 1 control connection + numTasks transfer connections
	connections, err := sess.AcquireConnectionsMulti(1+numTasks, false)
	if err != nil {
		if len(connections) == 0 {
			return xerrors.Errorf("failed to get %d connections, got %d: %w", 1+numTasks, len(connections), err)
		}

		logger.WithError(err).Debugf("failed to get %d connections, got %d", 1+numTasks, len(connections))
	}

	// if we have only one connection, use serial upload
	if len(connections) == 1 {
		// only one is available
		sess.ReturnConnection(connections[0])

		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	controlConn := connections[0]
	transferConns := connections[1:]

	defer sess.ReturnConnection(controlConn)

	for _, conn := range connections {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	// adjust number of tasks
	if numTasks != len(transferConns) {
		logger.Debugf("adjust number of tasks from %d to %d", numTasks, len(transferConns))
		numTasks = len(transferConns)
	}

	logger.Debugf("upload data object in parallel, size(%d), threads(%d)", fileLength, numTasks)

	// open a new file
	handle, err := OpenDataObjectForPutParallel(controlConn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, numTasks, fileLength, keywords)
	if err != nil {
		return err
	}

	replicaToken, resourceHierarchy, err := GetReplicaAccessInfo(controlConn, handle)
	if err != nil {
		CloseDataObject(controlConn, handle)
		return err
	}

	logger.Debugf("replicaToken %s, resourceHierarchy %s", replicaToken, resourceHierarchy)

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", atomic.LoadInt64(&totalBytesUploaded), fileLength)
	}

	uploadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"local_path":  localPath,
			"irods_path":  irodsPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("uploading data object partition")

		// close transfer connection after use
		defer func() {
			sess.DiscardConnection(transferConn)
			taskWaitGroup.Done()
		}()

		// open the file with read-write mode
		// to not seek to end
		taskHandle, _, taskErr := OpenDataObjectWithReplicaToken(transferConn, irodsPath, resource, "w", replicaToken, resourceHierarchy, numTasks, fileLength, keywords)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer func() {
			errClose := CloseDataObjectReplica(transferConn, taskHandle)
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

		taskNewOffset, taskErr := SeekDataObject(transferConn, taskHandle, taskOffset, types.SeekSet)
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
				taskWriteErr = WriteDataObjectWithTrackerCallBack(transferConn, taskHandle, buffer[:bytesRead], nil)
				if taskWriteErr != nil {
					break
				}

				atomic.AddInt64(&totalBytesUploaded, int64(bytesRead))
				if transferCallback != nil {
					transferCallback("upload", atomic.LoadInt64(&totalBytesUploaded), fileLength)
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

		go uploadTask(i, transferConns[i], offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		CloseDataObject(controlConn, handle)
		return <-errChan
	}

	err = CloseDataObject(controlConn, handle)
	if err != nil {
		return err
	}

	// replicate
	if replicate {
		err = ReplicateDataObject(controlConn, irodsPath, "", true, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// UploadDataObjectParallelWithConnections put a data object at the local path to the iRODS path in parallel
// Partitions a file into n (taskNum) tasks and uploads in parallel
func UploadDataObjectParallelWithConnections(conns []*connection.IRODSConnection, localPath string, irodsPath string, resource string, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"local_path": localPath,
		"irods_path": irodsPath,
		"resource":   resource,
		"replicate":  replicate,
	})

	if len(conns) == 0 {
		return xerrors.Errorf("no connections provided")
	}

	for _, conn := range conns {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	if !conns[0].SupportParallelUpload() {
		// serial upload
		return UploadDataObjectWithConnection(conns[0], localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conns[0].GetAccount()
		resource = account.DefaultResource
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return xerrors.Errorf("failed to stat file %q: %w", localPath, err)
	}

	fileLength := stat.Size()

	if fileLength == 0 {
		// empty file
		return UploadDataObjectWithConnection(conns[0], localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	// if we have only one data connection, use serial upload
	if len(conns) < 2 {
		// serial upload
		return UploadDataObjectWithConnection(conns[0], localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	controlConn := conns[0]
	transferConns := conns[1:]
	numTasks := len(transferConns)

	logger.Debugf("upload data object in parallel, size(%d), threads(%d)", fileLength, numTasks)

	// open a new file
	handle, err := OpenDataObjectForPutParallel(controlConn, irodsPath, resource, "w+", common.OPER_TYPE_NONE, numTasks, fileLength, keywords)
	if err != nil {
		return err
	}

	replicaToken, resourceHierarchy, err := GetReplicaAccessInfo(controlConn, handle)
	if err != nil {
		CloseDataObject(controlConn, handle)
		return err
	}

	logger.Debugf("replicaToken %s, resourceHierarchy %s", replicaToken, resourceHierarchy)

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", atomic.LoadInt64(&totalBytesUploaded), fileLength)
	}

	uploadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"local_path":  localPath,
			"irods_path":  irodsPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("uploading data object partition")

		defer taskWaitGroup.Done()

		// open the file with read-write mode
		// to not seek to end
		taskHandle, _, taskErr := OpenDataObjectWithReplicaToken(transferConn, irodsPath, resource, "w", replicaToken, resourceHierarchy, numTasks, fileLength, keywords)
		if taskErr != nil {
			errChan <- taskErr
			return
		}
		defer func() {
			errClose := CloseDataObjectReplica(transferConn, taskHandle)
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

		taskNewOffset, taskErr := SeekDataObject(transferConn, taskHandle, taskOffset, types.SeekSet)
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
				taskWriteErr = WriteDataObjectWithTrackerCallBack(transferConn, taskHandle, buffer[:bytesRead], nil)
				if taskWriteErr != nil {
					break
				}

				atomic.AddInt64(&totalBytesUploaded, int64(bytesRead))
				if transferCallback != nil {
					transferCallback("upload", atomic.LoadInt64(&totalBytesUploaded), fileLength)
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

		go uploadTask(i, transferConns[i], offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		CloseDataObject(controlConn, handle)
		return <-errChan
	}

	err = CloseDataObject(controlConn, handle)
	if err != nil {
		return err
	}

	// replicate
	if replicate {
		err = ReplicateDataObject(controlConn, irodsPath, "", true, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// DownloadDataObjectToBuffer downloads a data object at the iRODS path to buffer
func DownloadDataObjectToBuffer(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, buffer *bytes.Buffer, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"irods_path": dataObject.Path,
		"resource":   resource,
	})

	logger.Debug("download data object")

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	conn, err := sess.AcquireConnection(true)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	defer sess.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, _, err := OpenDataObject(conn, dataObject.Path, resource, "r", keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", dataObject.Path, err)
	}
	defer CloseDataObject(conn, handle)

	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	// block read call-back
	var blockReadCallback common.TransferTrackerCallback
	if transferCallback != nil {
		blockReadCallback = func(taskName string, processed int64, total int64) {
			transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded)+processed, dataObject.Size)
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

			atomic.AddInt64(&totalBytesDownloaded, int64(bytesRead))
			if transferCallback != nil {
				transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				return xerrors.Errorf("failed to read data object %q: %w", dataObject.Path, readErr)
			}
		}
	}

	if writeErr != nil {
		return writeErr
	}

	return nil
}

// DownloadDataObjectToBufferWithConnection downloads a data object at the iRODS path to buffer
func DownloadDataObjectToBufferWithConnection(conn *connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, buffer *bytes.Buffer, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	handle, _, err := OpenDataObject(conn, dataObject.Path, resource, "r", keywords)
	if err != nil {
		return xerrors.Errorf("failed to open data object %q: %w", dataObject.Path, err)
	}
	defer CloseDataObject(conn, handle)

	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	// block read call-back
	var blockReadCallback common.TransferTrackerCallback
	if transferCallback != nil {
		blockReadCallback = func(taskName string, processed int64, total int64) {
			transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded)+processed, dataObject.Size)
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

			atomic.AddInt64(&totalBytesDownloaded, int64(bytesRead))
			if transferCallback != nil {
				transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			} else {
				return xerrors.Errorf("failed to read data object %q: %w", dataObject.Path, readErr)
			}
		}
	}

	if writeErr != nil {
		return writeErr
	}

	return nil
}

// DownloadDataObject downloads a data object at the iRODS path to the local path
func DownloadDataObject(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	return DownloadDataObjectParallel(sess, dataObject, resource, localPath, 1, keywords, transferCallback)
}

// DownloadDataObjectWithConnection downloads a data object at the iRODS path to the local path
func DownloadDataObjectWithConnection(conn *connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	conns := []*connection.IRODSConnection{conn}
	return DownloadDataObjectParallelWithConnections(conns, dataObject, resource, localPath, keywords, transferCallback)
}

// DownloadDataObjectResumable downloads a data object at the iRODS path to the local path with support of transfer resume
func DownloadDataObjectResumable(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	return DownloadDataObjectParallelResumable(sess, dataObject, resource, localPath, 1, keywords, transferCallback)
}

// DownloadDataObjectResumableWithConnection downloads a data object at the iRODS path to the local path with support of transfer resume
func DownloadDataObjectResumableWithConnection(conn *connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	conns := []*connection.IRODSConnection{conn}
	return DownloadDataObjectParallelResumableWithConnections(conns, dataObject, resource, localPath, keywords, transferCallback)
}

// DownloadDataObjectParallel downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallel(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, localPath string, taskNum int, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"irods_path": dataObject.Path,
		"resource":   resource,
		"local_path": localPath,
		"task_num":   taskNum,
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		// create an empty file
		f, err := os.Create(localPath)
		if err != nil {
			return xerrors.Errorf("failed to create file %q: %w", localPath, err)
		}
		f.Close()
		return nil
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObject.Size)
	}

	// acquire all transferConns
	// numTasks transfer transferConns
	// control connection is not needed
	transferConns, err := sess.AcquireConnectionsMulti(numTasks, false)
	if err != nil {
		if len(transferConns) == 0 {
			return xerrors.Errorf("failed to get %d connections, got %d: %w", numTasks, len(transferConns), err)
		}

		logger.WithError(err).Debugf("failed to get %d connections, got %d", numTasks, len(transferConns))
	}

	for _, conn := range transferConns {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	// adjust number of tasks
	if numTasks != len(transferConns) {
		logger.Debugf("adjust number of tasks from %d to %d", numTasks, len(transferConns))
		numTasks = len(transferConns)
	}

	logger.Debugf("downloading data object in parallel %s, size(%d), threads(%d)", dataObject.Path, dataObject.Size, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	currentBytesDownloaded := make([]int64, numTasks)
	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	downloadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"irods_path":  dataObject.Path,
			"local_path":  localPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("downloading data object partition")

		atomic.StoreInt64(&currentBytesDownloaded[taskID], 0)

		// close transfer connection after use
		defer func() {
			sess.ReturnConnection(transferConn)
			taskWaitGroup.Done()
		}()

		f, openErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if openErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, openErr)
			return
		}
		defer f.Close()

		lastOffset := int64(taskOffset)

		calcProgress := func(processed int64) {
			atomic.StoreInt64(&currentBytesDownloaded[taskID], processed)

			newTotal := int64(0)
			for i := 0; i < numTasks; i++ {
				newTotal += atomic.LoadInt64(&currentBytesDownloaded[i])
			}

			atomic.StoreInt64(&totalBytesDownloaded, newTotal)
		}

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				calcProgress(processed)

				if transferCallback != nil {
					transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
				}
			}
		}

		taskRemain := taskLength

		buffer := make([]byte, common.ReadWriteBufferSize)

		attempt := func(attemptConn *connection.IRODSConnection) error {
			attemptHandle, _, openErr := OpenDataObject(attemptConn, dataObject.Path, resource, "r", keywords)
			if openErr != nil {
				return openErr
			}

			defer func() {
				if !attemptConn.IsSocketFailed() && attemptConn.IsConnected() {
					CloseDataObject(attemptConn, attemptHandle)
				}
			}()

			// seek to task offset
			if lastOffset > 0 {
				taskLogger.Debugf("resuming downloading data object partition, last offset %d", lastOffset)

				newOffset, seekErr := SeekDataObject(attemptConn, attemptHandle, lastOffset, types.SeekSet)
				if seekErr != nil {
					return xerrors.Errorf("failed to seek data object %q to offset %d: %w", dataObject.Path, lastOffset, seekErr)
				}

				attemptNewOffset, localSeekErr := f.Seek(lastOffset, io.SeekStart)
				if localSeekErr != nil {
					return xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, localSeekErr)
				}

				if newOffset != attemptNewOffset {
					return xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
				}
			}

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, attemptReadErr := ReadDataObjectWithTrackerCallBack(attemptConn, attemptHandle, buffer[:bufferLen], blockReadCallback)
				if bytesRead > 0 {
					_, attemptWriteErr := f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
					if attemptWriteErr != nil {
						return xerrors.Errorf("failed to write to file %q from task %d: %w", localPath, taskID, attemptWriteErr)
					}

					calcProgress(int64(bytesRead))

					if transferCallback != nil {
						transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
					}

					taskRemain -= int64(bytesRead)
					lastOffset += int64(bytesRead)
				}

				if attemptReadErr != nil {
					if attemptReadErr == io.EOF {
						return nil
					}

					return xerrors.Errorf("failed to read from data object %q: %w", dataObject.Path, attemptReadErr)
				}

				if len(errChan) > 0 {
					// other tasks failed
					return xerrors.Errorf("stop running as other tasks failed")
				}
			}

			return nil
		}

		for {
			attemptErr := attempt(transferConn)
			if attemptErr == nil {
				// done downloading
				return
			}

			if transferConn.IsSocketFailed() {
				// retry
				taskLogger.WithError(attemptErr).Errorf("socket failed, retrying...")

				connErr := transferConn.Reconnect()
				if connErr != nil {
					errChan <- xerrors.Errorf("failed to reconnect: %w", connErr)
					return
				}

				if !transferConn.IsConnected() {
					errChan <- xerrors.Errorf("connection is disconnected")
					return
				}
			} else {
				// other errors
				errChan <- attemptErr
				return
			}
		}
	}

	lengthPerThread := dataObject.Size / int64(numTasks)
	if dataObject.Size%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, transferConns[i], offset, lengthPerThread)
		offset += lengthPerThread
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// DownloadDataObjectParallelWithConnections downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallelWithConnections(conns []*connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"irods_path": dataObject.Path,
		"resource":   resource,
		"local_path": localPath,
	})

	if len(conns) == 0 {
		return xerrors.Errorf("no connections provided")
	}

	for _, conn := range conns {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conns[0].GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		// create an empty file
		f, err := os.Create(localPath)
		if err != nil {
			return xerrors.Errorf("failed to create file %q: %w", localPath, err)
		}
		f.Close()
		return nil
	}

	transferConns := conns[:]
	numTasks := len(transferConns)

	logger.Debugf("downloading data object in parallel, size(%d), threads(%d)", dataObject.Size, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	currentBytesDownloaded := make([]int64, numTasks)
	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	downloadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"irods_path":  dataObject.Path,
			"local_path":  localPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("downloading data object partition")

		atomic.StoreInt64(&currentBytesDownloaded[taskID], 0)

		defer taskWaitGroup.Done()

		f, openErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if openErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, openErr)
			return
		}
		defer f.Close()

		lastOffset := int64(taskOffset)

		calcProgress := func(processed int64) {
			atomic.StoreInt64(&currentBytesDownloaded[taskID], processed)

			newTotal := int64(0)
			for i := 0; i < numTasks; i++ {
				newTotal += atomic.LoadInt64(&currentBytesDownloaded[i])
			}

			atomic.StoreInt64(&totalBytesDownloaded, newTotal)
		}

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				calcProgress(processed)

				if transferCallback != nil {
					transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
				}
			}
		}

		taskRemain := taskLength

		buffer := make([]byte, common.ReadWriteBufferSize)

		attempt := func(attemptConn *connection.IRODSConnection) error {
			attemptHandle, _, openErr := OpenDataObject(attemptConn, dataObject.Path, resource, "r", keywords)
			if openErr != nil {
				return openErr
			}

			defer func() {
				if !attemptConn.IsSocketFailed() && attemptConn.IsConnected() {
					CloseDataObject(attemptConn, attemptHandle)
				}
			}()

			// seek to task offset
			if lastOffset > 0 {
				taskLogger.Debugf("resuming downloading data object partition, last offset %d", lastOffset)

				newOffset, seekErr := SeekDataObject(attemptConn, attemptHandle, lastOffset, types.SeekSet)
				if seekErr != nil {
					return xerrors.Errorf("failed to seek data object %q to offset %d: %w", dataObject.Path, lastOffset, seekErr)
				}

				attemptNewOffset, localSeekErr := f.Seek(lastOffset, io.SeekStart)
				if localSeekErr != nil {
					return xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, localSeekErr)
				}

				if newOffset != attemptNewOffset {
					return xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
				}
			}

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, attemptReadErr := ReadDataObjectWithTrackerCallBack(attemptConn, attemptHandle, buffer[:bufferLen], blockReadCallback)
				if bytesRead > 0 {
					_, attemptWriteErr := f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
					if attemptWriteErr != nil {
						return xerrors.Errorf("failed to write to file %q from task %d: %w", localPath, taskID, attemptWriteErr)
					}

					calcProgress(int64(bytesRead))

					if transferCallback != nil {
						transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
					}

					taskRemain -= int64(bytesRead)
					lastOffset += int64(bytesRead)
				}

				if attemptReadErr != nil {
					if attemptReadErr == io.EOF {
						return nil
					}

					return xerrors.Errorf("failed to read from data object %q: %w", dataObject.Path, attemptReadErr)
				}

				if len(errChan) > 0 {
					// other tasks failed
					return xerrors.Errorf("stop running as other tasks failed")
				}
			}

			return nil
		}

		for {
			attemptErr := attempt(transferConn)
			if attemptErr == nil {
				// done downloading
				return
			}

			if transferConn.IsSocketFailed() {
				// retry
				taskLogger.WithError(attemptErr).Errorf("socket failed, retrying...")

				connErr := transferConn.Reconnect()
				if connErr != nil {
					errChan <- xerrors.Errorf("failed to reconnect: %w", connErr)
					return
				}

				if !transferConn.IsConnected() {
					errChan <- xerrors.Errorf("connection is disconnected")
					return
				}
			} else {
				// other errors
				errChan <- attemptErr
				return
			}
		}
	}

	lengthPerThread := dataObject.Size / int64(numTasks)
	if dataObject.Size%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, transferConns[i], offset, lengthPerThread)
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
// TODO: Need to partition a file in small chunks so that different number of tasks can be used to continue downloading
func DownloadDataObjectParallelResumable(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, localPath string, taskNum int, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"irods_path": dataObject.Path,
		"resource":   resource,
		"local_path": localPath,
		"task_num":   taskNum,
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		// create an empty file
		f, err := os.Create(localPath)
		if err != nil {
			return xerrors.Errorf("failed to create file %q: %w", localPath, err)
		}
		f.Close()
		return nil
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObject.Size)
	}

	// acquire all transferConns
	// numTasks transfer transferConns
	// control connection is not needed
	transferConns, err := sess.AcquireConnectionsMulti(numTasks, false)
	if err != nil {
		if len(transferConns) == 0 {
			return xerrors.Errorf("failed to get %d connections, got %d: %w", numTasks, len(transferConns), err)
		}

		logger.WithError(err).Debugf("failed to get %d connections, got %d", numTasks, len(transferConns))
	}

	for _, conn := range transferConns {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	// adjust number of tasks
	if numTasks != len(transferConns) {
		logger.Debugf("adjust number of tasks from %d to %d", numTasks, len(transferConns))
		numTasks = len(transferConns)
	}

	// create transfer status
	transferStatusLocal, err := GetOrNewDataObjectTransferStatusLocal(localPath, dataObject.Size, numTasks)
	if err != nil {
		return xerrors.Errorf("failed to read transfer status file for %q: %w", localPath, err)
	}

	logger.Debugf("downloading data object in parallel, size(%d), threads(%d)", dataObject.Size, numTasks)

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

	currentBytesDownloaded := make([]int64, numTasks)
	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	downloadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"irods_path":  dataObject.Path,
			"local_path":  localPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("downloading data object partition")

		atomic.StoreInt64(&currentBytesDownloaded[taskID], 0)

		// close transfer connection after use
		defer func() {
			sess.ReturnConnection(transferConn)
			taskWaitGroup.Done()
		}()

		f, openErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if openErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, openErr)
			return
		}
		defer f.Close()

		// find last failure point
		transferStatus := transferStatusLocal.GetStatus()
		lastOffset := int64(taskOffset)
		if transferStatus != nil {
			if transferStatusEntry, ok := transferStatus.StatusMap[taskOffset]; ok {
				lastOffset = transferStatusEntry.StartOffset + transferStatusEntry.CompletedLength
			}
		}

		calcProgress := func(processed int64) {
			atomic.StoreInt64(&currentBytesDownloaded[taskID], processed)

			newTotal := int64(0)
			for i := 0; i < numTasks; i++ {
				newTotal += atomic.LoadInt64(&currentBytesDownloaded[i])
			}

			atomic.StoreInt64(&totalBytesDownloaded, newTotal)
		}

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				calcProgress(processed)

				if transferCallback != nil {
					transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
				}
			}
		}

		if lastOffset-taskOffset > 0 {
			blockReadCallback("download", lastOffset-taskOffset, taskLength)
		}

		taskRemain := taskLength - (lastOffset - taskOffset)

		buffer := make([]byte, common.ReadWriteBufferSize)

		attempt := func(attemptConn *connection.IRODSConnection) error {
			attemptHandle, _, openErr := OpenDataObject(attemptConn, dataObject.Path, resource, "r", keywords)
			if openErr != nil {
				return openErr
			}

			defer func() {
				if !attemptConn.IsSocketFailed() && attemptConn.IsConnected() {
					CloseDataObject(attemptConn, attemptHandle)
				}
			}()

			// seek to last offset
			if lastOffset > 0 {
				taskLogger.Debugf("resuming downloading data object partition, last offset %d", lastOffset)

				newOffset, seekErr := SeekDataObject(attemptConn, attemptHandle, lastOffset, types.SeekSet)
				if seekErr != nil {
					return xerrors.Errorf("failed to seek data object %q to offset %d: %w", dataObject.Path, lastOffset, seekErr)
				}

				attemptNewOffset, localSeekErr := f.Seek(lastOffset, io.SeekStart)
				if localSeekErr != nil {
					return xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, localSeekErr)
				}

				if newOffset != attemptNewOffset {
					return xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
				}
			}

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, attemptReadErr := ReadDataObjectWithTrackerCallBack(attemptConn, attemptHandle, buffer[:bufferLen], blockReadCallback)
				if bytesRead > 0 {
					_, attemptWriteErr := f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
					if attemptWriteErr != nil {
						return xerrors.Errorf("failed to write to file %q from task %d: %w", localPath, taskID, attemptWriteErr)
					}

					calcProgress(int64(bytesRead))

					// write status
					transferStatusEntry := &DataObjectTransferStatusEntry{
						StartOffset:     taskOffset,
						Length:          taskLength,
						CompletedLength: (taskLength - taskRemain) + int64(bytesRead),
					}
					transferStatusLocal.WriteStatus(transferStatusEntry) //nolint

					if transferCallback != nil {
						transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
					}

					taskRemain -= int64(bytesRead)
					lastOffset += int64(bytesRead)
				}

				if attemptReadErr != nil {
					if attemptReadErr == io.EOF {
						return nil
					}

					return xerrors.Errorf("failed to read from data object %q: %w", dataObject.Path, attemptReadErr)
				}

				if len(errChan) > 0 {
					// other tasks failed
					return xerrors.Errorf("stop running as other tasks failed")
				}
			}

			return nil
		}

		for {
			attemptErr := attempt(transferConn)
			if attemptErr == nil {
				// done downloading
				return
			}

			if transferConn.IsSocketFailed() {
				// retry
				taskLogger.WithError(attemptErr).Errorf("socket failed, retrying...")

				connErr := transferConn.Reconnect()
				if connErr != nil {
					errChan <- xerrors.Errorf("failed to reconnect: %w", connErr)
					return
				}

				if !transferConn.IsConnected() {
					errChan <- xerrors.Errorf("connection is disconnected")
					return
				}
			} else {
				// other errors
				errChan <- attemptErr
				return
			}
		}
	}

	lengthPerThread := dataObject.Size / int64(numTasks)
	if dataObject.Size%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, transferConns[i], offset, lengthPerThread)
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

// DownloadDataObjectParallelResumable downloads a data object at the iRODS path to the local path in parallel with support of transfer resume
// Partitions a file into n (taskNum) tasks and downloads in parallel
// TODO: Need to partition a file in small chunks so that different number of tasks can be used to continue downloading
func DownloadDataObjectParallelResumableWithConnections(conns []*connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, localPath string, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"irods_path": dataObject.Path,
		"resource":   resource,
		"local_path": localPath,
	})

	if len(conns) == 0 {
		return xerrors.Errorf("no connections provided")
	}

	for _, conn := range conns {
		if conn == nil || !conn.IsConnected() {
			return xerrors.Errorf("connection is nil or disconnected")
		}
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conns[0].GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		// create an empty file
		f, err := os.Create(localPath)
		if err != nil {
			return xerrors.Errorf("failed to create file %q: %w", localPath, err)
		}
		f.Close()
		return nil
	}

	transferConns := conns[:]
	numTasks := len(transferConns)

	// create transfer status
	transferStatusLocal, err := GetOrNewDataObjectTransferStatusLocal(localPath, dataObject.Size, numTasks)
	if err != nil {
		return xerrors.Errorf("failed to read transfer status file for %q: %w", localPath, err)
	}

	logger.Debug("downloading data object in parallel")

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

	currentBytesDownloaded := make([]int64, numTasks)
	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
	}

	downloadTask := func(taskID int, transferConn *connection.IRODSConnection, taskOffset int64, taskLength int64) {
		taskLogger := log.WithFields(log.Fields{
			"irods_path":  dataObject.Path,
			"local_path":  localPath,
			"task_id":     taskID,
			"task_offset": taskOffset,
			"task_length": taskLength,
		})

		taskLogger.Debug("downloading data object partition")

		atomic.StoreInt64(&currentBytesDownloaded[taskID], 0)

		defer taskWaitGroup.Done()

		f, openErr := os.OpenFile(localPath, os.O_WRONLY, 0)
		if openErr != nil {
			errChan <- xerrors.Errorf("failed to open file %q: %w", localPath, openErr)
			return
		}
		defer f.Close()

		// find last failure point
		transferStatus := transferStatusLocal.GetStatus()
		lastOffset := int64(taskOffset)
		if transferStatus != nil {
			if transferStatusEntry, ok := transferStatus.StatusMap[taskOffset]; ok {
				lastOffset = transferStatusEntry.StartOffset + transferStatusEntry.CompletedLength
			}
		}

		calcProgress := func(processed int64) {
			atomic.StoreInt64(&currentBytesDownloaded[taskID], processed)

			newTotal := int64(0)
			for i := 0; i < numTasks; i++ {
				newTotal += atomic.LoadInt64(&currentBytesDownloaded[i])
			}

			atomic.StoreInt64(&totalBytesDownloaded, newTotal)
		}

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				calcProgress(processed)

				if transferCallback != nil {
					transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
				}
			}
		}

		if lastOffset-taskOffset > 0 {
			blockReadCallback("download", lastOffset-taskOffset, taskLength)
		}

		taskRemain := taskLength - (lastOffset - taskOffset)

		buffer := make([]byte, common.ReadWriteBufferSize)

		attempt := func(attemptConn *connection.IRODSConnection) error {
			attemptHandle, _, openErr := OpenDataObject(attemptConn, dataObject.Path, resource, "r", keywords)
			if openErr != nil {
				return openErr
			}

			defer func() {
				if !attemptConn.IsSocketFailed() && attemptConn.IsConnected() {
					CloseDataObject(attemptConn, attemptHandle)
				}
			}()

			// seek to last offset
			if lastOffset > 0 {
				taskLogger.Debugf("resuming downloading data object partition, last offset %d", lastOffset)

				newOffset, seekErr := SeekDataObject(attemptConn, attemptHandle, lastOffset, types.SeekSet)
				if seekErr != nil {
					return xerrors.Errorf("failed to seek data object %q to offset %d: %w", dataObject.Path, lastOffset, seekErr)
				}

				attemptNewOffset, localSeekErr := f.Seek(lastOffset, io.SeekStart)
				if localSeekErr != nil {
					return xerrors.Errorf("failed to seek file %q to offset %d: %w", localPath, lastOffset, localSeekErr)
				}

				if newOffset != attemptNewOffset {
					return xerrors.Errorf("failed to seek file and data object to target offset %d", lastOffset)
				}
			}

			// copy
			for taskRemain > 0 {
				bufferLen := common.ReadWriteBufferSize
				if taskRemain < int64(bufferLen) {
					bufferLen = int(taskRemain)
				}

				bytesRead, attemptReadErr := ReadDataObjectWithTrackerCallBack(attemptConn, attemptHandle, buffer[:bufferLen], blockReadCallback)
				if bytesRead > 0 {
					_, attemptWriteErr := f.WriteAt(buffer[:bytesRead], taskOffset+(taskLength-taskRemain))
					if attemptWriteErr != nil {
						return xerrors.Errorf("failed to write to file %q from task %d: %w", localPath, taskID, attemptWriteErr)
					}

					calcProgress(int64(bytesRead))

					// write status
					transferStatusEntry := &DataObjectTransferStatusEntry{
						StartOffset:     taskOffset,
						Length:          taskLength,
						CompletedLength: (taskLength - taskRemain) + int64(bytesRead),
					}
					transferStatusLocal.WriteStatus(transferStatusEntry) //nolint

					if transferCallback != nil {
						transferCallback("download", atomic.LoadInt64(&totalBytesDownloaded), dataObject.Size)
					}

					taskRemain -= int64(bytesRead)
					lastOffset += int64(bytesRead)
				}

				if attemptReadErr != nil {
					if attemptReadErr == io.EOF {
						return nil
					}

					return xerrors.Errorf("failed to read from data object %q: %w", dataObject.Path, attemptReadErr)
				}

				if len(errChan) > 0 {
					// other tasks failed
					return xerrors.Errorf("stop running as other tasks failed")
				}
			}

			return nil
		}

		for {
			attemptErr := attempt(transferConn)
			if attemptErr == nil {
				// done downloading
				return
			}

			if transferConn.IsSocketFailed() {
				// retry
				taskLogger.WithError(attemptErr).Errorf("socket failed, retrying...")

				connErr := transferConn.Reconnect()
				if connErr != nil {
					errChan <- xerrors.Errorf("failed to reconnect: %w", connErr)
					return
				}

				if !transferConn.IsConnected() {
					errChan <- xerrors.Errorf("connection is disconnected")
					return
				}
			} else {
				// other errors
				errChan <- attemptErr
				return
			}
		}
	}

	lengthPerThread := dataObject.Size / int64(numTasks)
	if dataObject.Size%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)

	for i := 0; i < numTasks; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i, transferConns[i], offset, lengthPerThread)
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
