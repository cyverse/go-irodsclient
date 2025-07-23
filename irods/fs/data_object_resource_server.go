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
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// GetDataObjectRedirectionInfoForGet returns a redirection info for accessing the data object for downloading
func GetDataObjectRedirectionInfoForGet(conn *connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, taskNum int, keywords map[common.KeyWord]string) (*types.IRODSFileOpenRedirectionHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObject.Size)
	}

	request := message.NewIRODSMessageGetDataObjectRequest(dataObject.Path, resource, dataObject.Size, numTasks)
	response := message.IRODSMessageGetDataObjectResponse{}

	for k, v := range keywords {
		request.AddKeyVal(k, v)
	}

	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return nil, xerrors.Errorf("failed to find the data object for path %q: %w", dataObject.Path, types.NewFileNotFoundError(dataObject.Path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", dataObject.Path, types.NewFileNotFoundError(dataObject.Path))
		}

		return nil, xerrors.Errorf("failed to get data object redirection info for path %q: %w", dataObject.Path, err)
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	info := &types.IRODSFileOpenRedirectionHandle{
		FileDescriptor:  response.FileDescriptor,
		Path:            dataObject.Path,
		Resource:        resource,
		Threads:         response.Threads,
		CheckSum:        response.CheckSum,
		RedirectionInfo: nil,
	}

	if response.PortList != nil {
		redirection := &types.IRODSRedirectionInfo{
			Port:         response.PortList.Port,
			Cookie:       response.PortList.Cookie,
			ServerSocket: response.PortList.ServerSocket,
			WindowSize:   response.PortList.WindowSize,
			Host:         response.PortList.HostAddress,
		}

		info.RedirectionInfo = redirection
	}

	return info, nil
}

// GetDataObjectRedirectionInfoForPut returns a redirection info for accessing the data object for uploading
func GetDataObjectRedirectionInfoForPut(conn *connection.IRODSConnection, path string, resource string, fileLength int64, taskNum int, keywords map[common.KeyWord]string) (*types.IRODSFileOpenRedirectionHandle, error) {
	if conn == nil || !conn.IsConnected() {
		return nil, xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectOpen(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := conn.GetAccount()
		resource = account.DefaultResource
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	request := message.NewIRODSMessagePutDataObjectRequest(path, resource, fileLength, numTasks)
	response := message.IRODSMessagePutDataObjectResponse{}

	for k, v := range keywords {
		request.AddKeyVal(k, v)
	}

	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return nil, xerrors.Errorf("failed to find the data object for path %q: %w", path, types.NewFileNotFoundError(path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return nil, xerrors.Errorf("failed to find the collection for path %q: %w", path, types.NewFileNotFoundError(path))
		}

		return nil, xerrors.Errorf("failed to get data object redirection info for path %q: %w", path, err)
	}

	if metrics != nil {
		metrics.IncreaseCounterForOpenFileHandles(1)
	}

	info := &types.IRODSFileOpenRedirectionHandle{
		FileDescriptor:  response.FileDescriptor,
		Path:            path,
		Resource:        resource,
		Threads:         response.Threads,
		CheckSum:        response.CheckSum,
		RedirectionInfo: nil,
	}

	if response.PortList != nil {
		redirection := &types.IRODSRedirectionInfo{
			Port:         response.PortList.Port,
			Cookie:       response.PortList.Cookie,
			ServerSocket: response.PortList.ServerSocket,
			WindowSize:   response.PortList.WindowSize,
			Host:         response.PortList.HostAddress,
		}

		info.RedirectionInfo = redirection
	}

	return info, nil
}

// CompleteDataObjectRedirection completes a redirection for accessing the data object for downloading and uploading
func CompleteDataObjectRedirection(conn *connection.IRODSConnection, handle *types.IRODSFileOpenRedirectionHandle) error {
	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	metrics := conn.GetMetrics()
	if metrics != nil {
		metrics.IncreaseCounterForDataObjectClose(1)
	}

	if metrics != nil {
		metrics.DecreaseCounterForOpenFileHandles(1)
	}

	// lock the connection
	conn.Lock()
	defer conn.Unlock()

	request := message.NewIRODSMessageGetDataObjectCompleteRequest(handle.FileDescriptor)
	response := message.IRODSMessageGetDataObjectCompleteResponse{}
	err := conn.RequestAndCheck(request, &response, nil, conn.GetOperationTimeout())
	if err != nil {
		if types.GetIRODSErrorCode(err) == common.CAT_NO_ROWS_FOUND || types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_FILE {
			return xerrors.Errorf("failed to complete data object redirection for path %q: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		} else if types.GetIRODSErrorCode(err) == common.CAT_UNKNOWN_COLLECTION {
			return xerrors.Errorf("failed to find the collection for path %q: %w", handle.Path, types.NewFileNotFoundError(handle.Path))
		}

		return xerrors.Errorf("failed to complete data object redirection for path %q: %w", handle.Path, err)
	}

	return nil
}

func downloadDataObjectChunkFromResourceServer(sess *session.IRODSSession, taskID int, controlConn *connection.IRODSConnection, handle *types.IRODSFileOpenRedirectionHandle, localPath string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "downloadDataObjectChunkFromResourceServer",
		"task":     taskID,
	})

	logger.Debugf("download data object %q, task %d", handle.Path, taskID)

	conn, err := sess.GetRedirectionConnection(controlConn, handle.RedirectionInfo)
	if err != nil {
		return xerrors.Errorf("failed to get connection to resource server: %w", err)
	}

	err = conn.Connect()
	if err != nil {
		return xerrors.Errorf("failed to connect to resource server: %w", err)
	}

	defer conn.Disconnect()

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	conn.Lock()
	defer conn.Unlock()

	f, taskErr := os.OpenFile(localPath, os.O_WRONLY, 0)
	if taskErr != nil {
		return xerrors.Errorf("failed to open file %q: %w", localPath, taskErr)
	}
	defer f.Close()

	// encConfig may be nil
	encConfig := controlConn.GetAccount().SSLConfiguration
	encKeysize := 0

	if controlConn.IsSSL() {
		encKeysize = encConfig.EncryptionKeySize
	}

	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", totalBytesDownloaded, -1)
	}

	cont := true

	// transfer header
	transferHeader := message.IRODSMessageResourceServerTransferHeader{}
	headerBuffer := make([]byte, transferHeader.SizeOf())

	var dataBuffer []byte
	var encryptedDataBuffer []byte

	timeout := controlConn.GetOperationTimeout()

	for cont {
		// read transfer header
		readLen, err := conn.Recv(headerBuffer, transferHeader.SizeOf(), &timeout.ResponseTimeout)
		if err != nil {
			if err == io.EOF {
				break
			}

			return xerrors.Errorf("failed to read transfer header from resource server: %w", err)
		}

		err = transferHeader.FromBytes(headerBuffer[:readLen])
		if err != nil {
			return xerrors.Errorf("failed to read transfer header from resource server: %w", err)
		}

		if transferHeader.OperationType == int(common.OPER_TYPE_DONE) {
			// break
			logger.Debugf("done downloading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
			break
		} else if transferHeader.OperationType != int(common.OPER_TYPE_GET_DATA_OBJ) {
			return xerrors.Errorf("invalid operation type %d received for transfer", transferHeader.OperationType)
		}

		logger.Debugf("downloading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)

		toGet := transferHeader.Length
		curOffset := transferHeader.Offset

		for toGet > 0 {
			// set timeout
			conn.SetReadTimeout(timeout.ResponseTimeout)

			// read encryption header
			if controlConn.IsSSL() {
				encryptionHeader := message.NewIRODSMessageResourceServerTransferEncryptionHeader(encKeysize)

				encryptionHeaderBuffer := make([]byte, encryptionHeader.SizeOf())
				eof := false
				readLen, err := conn.Recv(encryptionHeaderBuffer, encryptionHeader.SizeOf(), nil)
				if err != nil {
					if err == io.EOF {
						eof = true
						cont = false
					} else {
						return xerrors.Errorf("failed to read transfer encryption header from resource server: %w", err)
					}
				}

				if eof {
					break
				}

				err = encryptionHeader.FromBytes(encryptionHeaderBuffer[:readLen])
				if err != nil {
					return xerrors.Errorf("failed to read transfer encryption header from bytes: %w", err)
				}

				// done reading encryption header
				//logger.Debugf("encryption header's content len %d, block len %d, key len %d", encryptionHeader.Length, encBlocksize, encKeysize)

				// size is different as data is encrypted
				encryptedDataLen := encryptionHeader.Length - encKeysize
				if len(encryptedDataBuffer) < encryptedDataLen {
					encryptedDataBuffer = make([]byte, encryptedDataLen)
				}

				if len(dataBuffer) < encryptedDataLen {
					dataBuffer = make([]byte, encryptedDataLen)
				}

				//logger.Debugf("encrypted data len %d", encryptedDataLen)

				// read data
				readLen, err = conn.Recv(encryptedDataBuffer, encryptedDataLen, nil)
				if readLen > 0 {
					// decrypt
					decryptedDataLen, decErr := conn.Decrypt(encryptionHeader.IV, encryptedDataBuffer[:readLen], dataBuffer)
					if decErr != nil {
						return xerrors.Errorf("failed to decrypt data: %w", decErr)
					}

					//logger.Debugf("decrypted data len %d", decryptedDataLen)

					atomic.AddInt64(&totalBytesDownloaded, int64(decryptedDataLen))
					if transferCallback != nil {
						transferCallback("download", totalBytesDownloaded, -1)
					}

					_, writeErr := f.WriteAt(dataBuffer[:decryptedDataLen], curOffset)
					if writeErr != nil {
						return xerrors.Errorf("failed to write data to %q, task %d, offset %d: %w", localPath, taskID, curOffset, writeErr)
					}

					toGet -= int64(decryptedDataLen)
					curOffset += int64(decryptedDataLen)
				}

				if err != nil {
					if err == io.EOF {
						eof = true
						cont = false
					} else {
						return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, err)
					}
				}

				if eof {
					break
				}
			} else {
				// normal
				// read data
				newOffset, err := f.Seek(curOffset, io.SeekStart)
				if err != nil {
					return xerrors.Errorf("failed to seek to offset %d for file %q, task %d: %w", curOffset, localPath, taskID, err)
				}

				if newOffset != curOffset {
					return xerrors.Errorf("failed to seek to offset %d for file %q, task %d, new offset %d: %w", curOffset, localPath, taskID, newOffset, err)
				}

				eof := false
				readLen, err := conn.RecvToWriter(f, toGet, nil)
				if readLen > 0 {
					atomic.AddInt64(&totalBytesDownloaded, readLen)
					if transferCallback != nil {
						transferCallback("download", totalBytesDownloaded, -1)
					}

					toGet -= int64(readLen)
					curOffset += int64(readLen)
				}

				if err != nil {
					if err == io.EOF {
						eof = true
						cont = false
					} else {
						return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, err)
					}
				}

				if eof {
					break
				}
			}
		}
	}

	logger.Debugf("downloaded data object %q, task %d", handle.Path, taskID)

	return nil
}

func uploadDataObjectChunkToResourceServer(sess *session.IRODSSession, taskID int, controlConn *connection.IRODSConnection, handle *types.IRODSFileOpenRedirectionHandle, localPath string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "uploadDataObjectChunkToResourceServer",
		"task":     taskID,
	})

	logger.Debugf("upload data object %q, task %d", handle.Path, taskID)

	conn, err := sess.GetRedirectionConnection(controlConn, handle.RedirectionInfo)
	if err != nil {
		return xerrors.Errorf("failed to get connection to resource server: %w", err)
	}

	err = conn.Connect()
	if err != nil {
		return xerrors.Errorf("failed to connect to resource server: %w", err)
	}

	defer conn.Disconnect()

	if conn == nil || !conn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	conn.Lock()
	defer conn.Unlock()

	f, taskErr := os.OpenFile(localPath, os.O_RDONLY, 0)
	if taskErr != nil {
		return xerrors.Errorf("failed to open file %q: %w", localPath, taskErr)
	}
	defer f.Close()

	// encConfig may be nil
	encConfig := controlConn.GetAccount().SSLConfiguration
	encKeysize := 0

	if controlConn.IsSSL() {
		encKeysize = encConfig.EncryptionKeySize
	}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, -1)
	}

	cont := true

	// transfer header
	transferHeader := message.IRODSMessageResourceServerTransferHeader{}
	headerBuffer := make([]byte, transferHeader.SizeOf())

	var dataBuffer []byte
	var encryptedDataBuffer []byte
	dataBufferSize := sess.GetConfig().TcpBufferSize

	timeout := controlConn.GetOperationTimeout()

	for cont {
		// read transfer header
		readLen, err := conn.Recv(headerBuffer, transferHeader.SizeOf(), &timeout.ResponseTimeout)
		if err != nil {
			if err == io.EOF {
				break
			}

			return xerrors.Errorf("failed to read transfer header from resource server: %w", err)
		}

		err = transferHeader.FromBytes(headerBuffer[:readLen])
		if err != nil {
			return xerrors.Errorf("failed to read transfer header from resource server: %w", err)
		}

		if transferHeader.OperationType == int(common.OPER_TYPE_DONE) {
			// break
			logger.Debugf("done uploading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
			break
		} else if transferHeader.OperationType != int(common.OPER_TYPE_PUT_DATA_OBJ) {
			return xerrors.Errorf("invalid operation type %d received for transfer", transferHeader.OperationType)
		}

		logger.Debugf("uploading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)

		toPut := transferHeader.Length
		curOffset := transferHeader.Offset
		for toPut > 0 {
			// set timeout
			conn.SetWriteTimeout(timeout.RequestTimeout)

			// read encryption header
			if controlConn.IsSSL() {
				// init iv
				encAlg := types.GetEncryptionAlgorithm(encConfig.EncryptionAlgorithm)
				encIV, err := util.GetEncryptionIV(encAlg)
				if err != nil {
					return xerrors.Errorf("failed to get encryption iv: %w", err)
				}

				iv := make([]byte, encKeysize)
				copy(iv, encIV)

				encryptionHeader := message.NewIRODSMessageResourceServerTransferEncryptionHeader(encKeysize)
				encryptionHeader.IV = iv

				if len(dataBuffer) < dataBufferSize {
					// resize
					dataBuffer = make([]byte, dataBufferSize)
				}

				// size is different as data is encrypted
				if len(encryptedDataBuffer) < dataBufferSize*2 {
					encryptedDataBuffer = make([]byte, dataBufferSize*2)
				}

				// read data
				eof := false
				readLen, err := f.ReadAt(dataBuffer, curOffset)

				//logger.Debugf("read offset %d, len %d", curOffset, readLen)
				if readLen > 0 {
					// encrypt
					encLen, encErr := conn.Encrypt(iv, dataBuffer[:readLen], encryptedDataBuffer)
					if encErr != nil {
						return xerrors.Errorf("failed to encrypt data: %w", encErr)
					}

					//logger.Debugf("read offset %d, original len %d, encrypted len %d", curOffset, readLen, encLen)
					encryptionHeader.Length = encLen + encKeysize
				}

				if err != nil {
					if err == io.EOF {
						eof = true
						cont = false
					} else {
						return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", localPath, taskID, curOffset, err)
					}
				}

				encryptionHeaderBuffer, err := encryptionHeader.GetBytes()
				if err != nil {
					return xerrors.Errorf("failed to get bytes from transfer encryption header: %w", err)
				}

				//logger.Debugf("sending encryption header, header len %d, content len %d", len(encryptionHeaderBuffer), encryptionHeader.Length)
				err = conn.Send(encryptionHeaderBuffer, len(encryptionHeaderBuffer), nil)
				if err != nil {
					return xerrors.Errorf("failed to write transfer encryption header to resource server: %w", err)
				}

				//logger.Debugf("sending encrypted data")
				encryptedDataLen := encryptionHeader.Length - encKeysize
				writeErr := conn.Send(encryptedDataBuffer, encryptedDataLen, nil)
				if writeErr != nil {
					return xerrors.Errorf("failed to write data to %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, writeErr)
				}

				//logger.Debugf("sent encrypted data")

				atomic.AddInt64(&totalBytesUploaded, int64(readLen))
				if transferCallback != nil {
					transferCallback("upload", totalBytesUploaded, -1)
				}

				toPut -= int64(readLen)
				curOffset += int64(readLen)

				if eof {
					break
				}
			} else {
				// normal
				// write data
				newOffset, err := f.Seek(curOffset, io.SeekStart)
				if err != nil {
					return xerrors.Errorf("failed to seek to offset %d for file %q, task %d: %w", curOffset, localPath, taskID, err)
				}

				if newOffset != curOffset {
					return xerrors.Errorf("failed to seek to offset %d for file %q, task %d, new offset %d: %w", curOffset, localPath, taskID, newOffset, err)
				}

				eof := false
				putLen, err := conn.SendFromReader(f, toPut, nil)
				if putLen > 0 {
					atomic.AddInt64(&totalBytesUploaded, putLen)
					if transferCallback != nil {
						transferCallback("upload", totalBytesUploaded, -1)
					}

					toPut -= putLen
					curOffset += putLen
				}

				if err != nil {
					if err == io.EOF {
						eof = true
						cont = false
					} else {
						return xerrors.Errorf("failed to write data %q, task %d, offset %d: %w", localPath, taskID, transferHeader.Offset, err)
					}
				}

				if eof {
					break
				}
			}
		}
	}

	logger.Debugf("uploaded data object %q, task %d", handle.Path, taskID)

	return nil
}

// DownloadDataObjectFromResourceServer downloads a data object at the iRODS path to the local path
func DownloadDataObjectFromResourceServer(sess *session.IRODSSession, dataObject *types.IRODSDataObject, resource string, localPath string, taskNum int, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectFromResourceServer",
	})

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		return DownloadDataObject(sess, dataObject, resource, localPath, keywords, transferCallback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObject.Size)
	}

	if numTasks == 1 {
		return DownloadDataObject(sess, dataObject, resource, localPath, keywords, transferCallback)
	}

	controlConn, err := sess.AcquireConnection(true)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	controlConnReleased := false

	defer func() {
		if !controlConnReleased {
			// close control connection here
			sess.ReturnConnection(controlConn)
		}
	}()

	if controlConn == nil || !controlConn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, err := GetDataObjectRedirectionInfoForGet(controlConn, dataObject, resource, numTasks, keywords)
	if err != nil {
		// close control connection
		sess.ReturnConnection(controlConn)
		controlConnReleased = true

		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to DownloadDataObjectParallel", dataObject.Path)
		return DownloadDataObjectParallel(sess, dataObject, resource, localPath, numTasks, keywords, transferCallback)
	}

	logger.Debugf("download data object in parallel (redirect-to-resource) %s, size(%d), threads(%d)", dataObject.Path, dataObject.Size, numTasks)

	defer CompleteDataObjectRedirection(controlConn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		// close control connection
		sess.ReturnConnection(controlConn)
		controlConnReleased = true

		logger.Debugf("failed to get redirection info for data object %q, switch to DownloadDataObjectParallel", dataObject.Path)

		return DownloadDataObjectParallel(sess, dataObject, resource, localPath, numTasks, keywords, transferCallback)
	}

	numTasks = handle.Threads

	logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
	// get from portal

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", totalBytesDownloaded, dataObject.Size)
	}

	// task progress
	taskProgress := make([]int64, numTasks)

	downloadTask := func(taskID int) {
		taskProgress[taskID] = 0

		defer taskWaitGroup.Done()

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				atomic.AddInt64(&totalBytesDownloaded, int64(delta))

				if transferCallback != nil {
					transferCallback(taskName, totalBytesDownloaded, dataObject.Size)
				}
			}
		}

		err = downloadDataObjectChunkFromResourceServer(sess, taskID, controlConn, handle, localPath, blockReadCallback)
		if err != nil {
			dnErr := xerrors.Errorf("failed to download data object chunk %q from resource server: %w", dataObject.Path, err)
			errChan <- dnErr
		}
	}

	for i := 0; i < handle.Threads; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i)
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// DownloadDataObjectFromResourceServerWithConnection downloads a data object at the iRODS path to the local path
func DownloadDataObjectFromResourceServerWithConnection(sess *session.IRODSSession, controlConn *connection.IRODSConnection, dataObject *types.IRODSDataObject, resource string, localPath string, taskNum int, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectFromResourceServerWithConnection",
	})

	if controlConn == nil || !controlConn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := sess.GetAccount()
		resource = account.DefaultResource
	}

	if dataObject.Size == 0 {
		// empty file
		return DownloadDataObjectWithConnection(controlConn, dataObject, resource, localPath, keywords, transferCallback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObject.Size)
	}

	if numTasks == 1 {
		return DownloadDataObjectWithConnection(controlConn, dataObject, resource, localPath, keywords, transferCallback)
	}

	handle, err := GetDataObjectRedirectionInfoForGet(controlConn, dataObject, resource, numTasks, keywords)
	if err != nil {
		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to DownloadDataObject", dataObject.Path)
		return DownloadDataObjectWithConnection(controlConn, dataObject, resource, localPath, keywords, transferCallback)
	}

	logger.Debugf("download data object in parallel (redirect-to-resource) %s, size(%d), threads(%d)", dataObject.Path, dataObject.Size, numTasks)

	defer CompleteDataObjectRedirection(controlConn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		logger.Debugf("failed to get redirection info for data object %q, switch to DownloadDataObject", dataObject.Path)
		return DownloadDataObjectWithConnection(controlConn, dataObject, resource, localPath, keywords, transferCallback)
	}

	numTasks = handle.Threads

	logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
	// get from portal

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return xerrors.Errorf("failed to create file %q: %w", localPath, err)
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesDownloaded := int64(0)
	if transferCallback != nil {
		transferCallback("download", totalBytesDownloaded, dataObject.Size)
	}

	// task progress
	taskProgress := make([]int64, numTasks)

	downloadTask := func(taskID int) {
		taskProgress[taskID] = 0

		defer taskWaitGroup.Done()

		blockReadCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				atomic.AddInt64(&totalBytesDownloaded, int64(delta))

				if transferCallback != nil {
					transferCallback(taskName, totalBytesDownloaded, dataObject.Size)
				}
			}
		}

		err = downloadDataObjectChunkFromResourceServer(sess, taskID, controlConn, handle, localPath, blockReadCallback)
		if err != nil {
			dnErr := xerrors.Errorf("failed to download data object chunk %q from resource server: %w", dataObject.Path, err)
			errChan <- dnErr
		}
	}

	for i := 0; i < handle.Threads; i++ {
		taskWaitGroup.Add(1)

		go downloadTask(i)
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// UploadDataObjectToResourceServer uploads a data object at the local path to the iRODS path
func UploadDataObjectToResourceServer(sess *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectToResourceServer",
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
	if fileLength == 0 {
		// empty file
		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks == 1 {
		return UploadDataObject(sess, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	controlConn, err := sess.AcquireConnection(false)
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	controlConnReleased := false

	defer func() {
		if !controlConnReleased {
			// close control connection here
			sess.ReturnConnection(controlConn)
		}
	}()

	if controlConn == nil || !controlConn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, err := GetDataObjectRedirectionInfoForPut(controlConn, irodsPath, resource, fileLength, numTasks, keywords)
	if err != nil {
		// close control connection
		sess.ReturnConnection(controlConn)
		controlConnReleased = true

		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to UploadDataObjctParallel", irodsPath)
		return UploadDataObjectParallel(sess, localPath, irodsPath, resource, 0, replicate, keywords, transferCallback)
	}

	logger.Debugf("upload data object in parallel (redirect-to-resource) %s, size(%d), threads(%d)", irodsPath, fileLength, numTasks)

	defer CompleteDataObjectRedirection(controlConn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		// close control connection
		sess.ReturnConnection(controlConn)
		controlConnReleased = true

		logger.Debugf("failed to get redirection info for data object %q, switch to UploadDataObjectParallel", irodsPath)

		return UploadDataObjectParallel(sess, localPath, irodsPath, resource, numTasks, replicate, keywords, transferCallback)
	}

	numTasks = handle.Threads

	logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
	// put to portal

	errChan := make(chan error, handle.Threads)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// task progress
	taskProgress := make([]int64, handle.Threads)

	uploadTask := func(taskID int) {
		taskProgress[taskID] = 0

		defer taskWaitGroup.Done()

		blockWriteCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				atomic.AddInt64(&totalBytesUploaded, int64(delta))

				if transferCallback != nil {
					transferCallback(taskName, totalBytesUploaded, fileLength)
				}
			}
		}

		err = uploadDataObjectChunkToResourceServer(sess, taskID, controlConn, handle, localPath, blockWriteCallback)
		if err != nil {
			dnErr := xerrors.Errorf("failed to upload data object chunk %q to resource server: %w", localPath, err)
			errChan <- dnErr
		}
	}

	for i := 0; i < handle.Threads; i++ {
		taskWaitGroup.Add(1)

		go uploadTask(i)
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// UploadDataObjectToResourceServerWithConnection uploads a data object at the local path to the iRODS path
func UploadDataObjectToResourceServerWithConnection(sess *session.IRODSSession, controlConn *connection.IRODSConnection, localPath string, irodsPath string, resource string, taskNum int, replicate bool, keywords map[common.KeyWord]string, transferCallback common.TransferTrackerCallback) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectToResourceServerWithConnection",
	})

	if controlConn == nil || !controlConn.IsConnected() {
		return xerrors.Errorf("connection is nil or disconnected")
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
		return UploadDataObjectWithConnection(controlConn, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	if numTasks == 1 {
		return UploadDataObjectWithConnection(controlConn, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	handle, err := GetDataObjectRedirectionInfoForPut(controlConn, irodsPath, resource, fileLength, numTasks, keywords)
	if err != nil {
		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to UploadDataObjct", irodsPath)
		return UploadDataObjectWithConnection(controlConn, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	logger.Debugf("upload data object in parallel (redirect-to-resource) %s, size(%d), threads(%d)", irodsPath, fileLength, numTasks)

	defer CompleteDataObjectRedirection(controlConn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		logger.Debugf("failed to get redirection info for data object %q, switch to UploadDataObject", irodsPath)
		return UploadDataObjectWithConnection(controlConn, localPath, irodsPath, resource, replicate, keywords, transferCallback)
	}

	numTasks = handle.Threads

	logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
	// put to portal

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	totalBytesUploaded := int64(0)
	if transferCallback != nil {
		transferCallback("upload", totalBytesUploaded, fileLength)
	}

	// task progress
	taskProgress := make([]int64, numTasks)

	uploadTask := func(taskID int) {
		taskProgress[taskID] = 0

		defer taskWaitGroup.Done()

		blockWriteCallback := func(taskName string, processed int64, total int64) {
			if processed > 0 {
				delta := processed - taskProgress[taskID]
				taskProgress[taskID] = processed

				atomic.AddInt64(&totalBytesUploaded, int64(delta))

				if transferCallback != nil {
					transferCallback(taskName, totalBytesUploaded, fileLength)
				}
			}
		}

		err = uploadDataObjectChunkToResourceServer(sess, taskID, controlConn, handle, localPath, blockWriteCallback)
		if err != nil {
			dnErr := xerrors.Errorf("failed to upload data object chunk %q to resource server: %w", localPath, err)
			errChan <- dnErr
		}
	}

	for i := 0; i < handle.Threads; i++ {
		taskWaitGroup.Add(1)

		go uploadTask(i)
	}

	taskWaitGroup.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}
