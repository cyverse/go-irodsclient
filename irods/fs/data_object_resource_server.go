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
func GetDataObjectRedirectionInfoForGet(conn *connection.IRODSConnection, path string, resource string, fileLength int64, taskNum int, keywords map[common.KeyWord]string) (*types.IRODSFileOpenRedirectionHandle, error) {
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

	request := message.NewIRODSMessageGetDataObjectRequest(path, resource, fileLength, numTasks)
	response := message.IRODSMessageGetDataObjectResponse{}

	for k, v := range keywords {
		request.AddKeyVal(k, v)
	}

	err := conn.RequestAndCheck(request, &response, nil)
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

	err := conn.RequestAndCheck(request, &response, nil)
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
	err := conn.RequestAndCheck(request, &response, nil)
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

func downloadDataObjectChunkFromResourceServer(sess *session.IRODSSession, taskID int, controlConnection *connection.IRODSConnection, handle *types.IRODSFileOpenRedirectionHandle, localPath string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "downloadDataObjectChunkFromResourceServer",
	})

	logger.Debugf("download data object %q, task %d", handle.Path, taskID)

	conn := sess.GetRedirectionConnection(controlConnection, handle.RedirectionInfo)
	err := conn.Connect()
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
	encConfig := controlConnection.GetAccount().SSLConfiguration
	encKeysize := 0

	if controlConnection.IsSSL() {
		encKeysize = encConfig.EncryptionKeySize
	}

	totalBytesDownloaded := int64(0)
	if callback != nil {
		callback(totalBytesDownloaded, -1)
	}

	cont := true

	// transfer header
	transferHeader := message.IRODSMessageResourceServerTransferHeader{}
	headerBuffer := make([]byte, transferHeader.SizeOf())

	var dataBuffer []byte
	var encryptedDataBuffer []byte

	for cont {
		// read transfer header
		readLen, err := conn.Recv(headerBuffer, transferHeader.SizeOf())
		if err != nil {
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
			// read encryption header
			if controlConnection.IsSSL() {
				encryptionHeader := message.NewIRODSMessageResourceServerTransferEncryptionHeader(encKeysize)

				encryptionHeaderBuffer := make([]byte, encryptionHeader.SizeOf())
				readLen, err := conn.Recv(encryptionHeaderBuffer, encryptionHeader.SizeOf())
				if err != nil {
					return xerrors.Errorf("failed to read transfer encryption header from resource server: %w", err)
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
				readLen, err = conn.Recv(encryptedDataBuffer, encryptedDataLen)
				if readLen > 0 {
					// decrypt
					decryptedDataLen, decErr := conn.Decrypt(encryptionHeader.IV, encryptedDataBuffer[:readLen], dataBuffer)
					if decErr != nil {
						return xerrors.Errorf("failed to decrypt data: %w", decErr)
					}

					//logger.Debugf("decrypted data len %d", decryptedDataLen)

					atomic.AddInt64(&totalBytesDownloaded, int64(decryptedDataLen))
					if callback != nil {
						callback(totalBytesDownloaded, -1)
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
						logger.Debugf("received EOF for downloading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
						cont = false
						break
					}

					return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, err)
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

				readLen, err := conn.RecvToWriter(f, toGet)
				if readLen > 0 {
					atomic.AddInt64(&totalBytesDownloaded, readLen)
					if callback != nil {
						callback(totalBytesDownloaded, -1)
					}

					toGet -= int64(readLen)
					curOffset += int64(readLen)
				}

				if err != nil {
					if err == io.EOF {
						logger.Debugf("received EOF for downloading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
						cont = false
						break
					}

					return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, err)
				}
			}
		}
	}

	logger.Debugf("downloaded data object %q, task %d", handle.Path, taskID)

	return nil
}

func uploadDataObjectChunkToResourceServer(sess *session.IRODSSession, taskID int, controlConnection *connection.IRODSConnection, handle *types.IRODSFileOpenRedirectionHandle, localPath string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "uploadDataObjectChunkToResourceServer",
	})

	logger.Debugf("upload data object %q, task %d", handle.Path, taskID)

	conn := sess.GetRedirectionConnection(controlConnection, handle.RedirectionInfo)
	err := conn.Connect()
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
	encConfig := controlConnection.GetAccount().SSLConfiguration
	encKeysize := 0

	if controlConnection.IsSSL() {
		// set iv
		encKeysize = encConfig.EncryptionKeySize
	}

	totalBytesUploaded := int64(0)
	if callback != nil {
		callback(totalBytesUploaded, -1)
	}

	cont := true

	// transfer header
	transferHeader := message.IRODSMessageResourceServerTransferHeader{}
	headerBuffer := make([]byte, transferHeader.SizeOf())

	var dataBuffer []byte
	var encryptedDataBuffer []byte
	dataBufferSize := sess.GetConfig().TCPBufferSize

	for cont {
		// read transfer header
		readLen, err := conn.Recv(headerBuffer, transferHeader.SizeOf())
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
			// read encryption header
			if controlConnection.IsSSL() {
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
						logger.Debugf("received EOF for uploading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
						cont = false
						break
					}

					return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", localPath, taskID, curOffset, err)
				}

				encryptionHeaderBuffer, err := encryptionHeader.GetBytes()
				if err != nil {
					return xerrors.Errorf("failed to get bytes from transfer encryption header: %w", err)
				}

				//logger.Debugf("sending encryption header, header len %d, content len %d", len(encryptionHeaderBuffer), encryptionHeader.Length)
				err = conn.Send(encryptionHeaderBuffer, len(encryptionHeaderBuffer))
				if err != nil {
					return xerrors.Errorf("failed to write transfer encryption header to resource server: %w", err)
				}

				//logger.Debugf("sending encrypted data")
				encryptedDataLen := encryptionHeader.Length - encKeysize
				writeErr := conn.Send(encryptedDataBuffer, encryptedDataLen)
				if writeErr != nil {
					return xerrors.Errorf("failed to write data to %q, task %d, offset %d: %w", handle.Path, taskID, curOffset, writeErr)
				}

				//logger.Debugf("sent encrypted data")

				atomic.AddInt64(&totalBytesUploaded, int64(readLen))
				if callback != nil {
					callback(totalBytesUploaded, -1)
				}

				toPut -= int64(readLen)
				curOffset += int64(readLen)
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

				err = conn.SendFromReader(f, toPut)
				atomic.AddInt64(&totalBytesUploaded, toPut)
				if callback != nil {
					callback(totalBytesUploaded, -1)
				}

				toPut -= toPut
				curOffset += toPut

				if err != nil {
					if err == io.EOF {
						logger.Debugf("received EOF for uploading file chunk for %s, task %d, offset %d, length %d", handle.Path, taskID, transferHeader.Offset, transferHeader.Length)
						cont = false
						break
					}

					return xerrors.Errorf("failed to read data %q, task %d, offset %d: %w", localPath, taskID, transferHeader.Offset, err)
				}
			}
		}
	}

	logger.Debugf("uploaded data object %q, task %d", handle.Path, taskID)

	return nil
}

// DownloadDataObjectFromResourceServer downloads a data object at the iRODS path to the local path
func DownloadDataObjectFromResourceServer(session *session.IRODSSession, irodsPath string, resource string, localPath string, fileLength int64, taskNum int, keywords map[common.KeyWord]string, callback common.TrackerCallBack) (string, error) {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "DownloadDataObjectFromResourceServer",
	})

	logger.Debugf("download data object %q", irodsPath)

	// use default resource when resource param is empty
	if len(resource) == 0 {
		account := session.GetAccount()
		resource = account.DefaultResource
	}

	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(fileLength)
	}

	conn, err := session.AcquireConnection()
	if err != nil {
		return "", xerrors.Errorf("failed to get connection: %w", err)
	}

	if conn == nil || !conn.IsConnected() {
		session.ReturnConnection(conn)
		return "", xerrors.Errorf("connection is nil or disconnected")
	}

	handle, err := GetDataObjectRedirectionInfoForGet(conn, irodsPath, resource, fileLength, numTasks, keywords)
	if err != nil {
		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to DownloadDataObjectParallel", irodsPath)

		session.ReturnConnection(conn)
		return "", DownloadDataObjectParallel(session, irodsPath, resource, localPath, fileLength, 0, keywords, callback)
	}

	// we defer return connection here to not occupy connection when switched to DownloadDataObjectParallel
	defer session.ReturnConnection(conn)

	defer CompleteDataObjectRedirection(conn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		// get file
		err = DownloadDataObjectParallel(session, irodsPath, resource, localPath, fileLength, 0, keywords, callback)
		if err != nil {
			return "", xerrors.Errorf("failed to download data object %q from resource server: %w", irodsPath, err)
		}
		return "", nil
	} else if handle.RedirectionInfo != nil {
		logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
		// get from portal

		// create an empty file
		f, err := os.Create(localPath)
		if err != nil {
			return "", xerrors.Errorf("failed to create file %q: %w", localPath, err)
		}
		f.Close()

		errChan := make(chan error, handle.Threads)
		taskWaitGroup := sync.WaitGroup{}

		totalBytesDownloaded := int64(0)
		if callback != nil {
			callback(totalBytesDownloaded, fileLength)
		}

		// task progress
		taskProgress := make([]int64, handle.Threads)

		downloadTask := func(taskID int) {
			taskProgress[taskID] = 0

			defer taskWaitGroup.Done()

			blockReadCallback := func(processed int64, total int64) {
				if processed > 0 {
					delta := processed - taskProgress[taskID]
					taskProgress[taskID] = processed

					atomic.AddInt64(&totalBytesDownloaded, int64(delta))

					if callback != nil {
						callback(totalBytesDownloaded, fileLength)
					}
				}
			}

			err = downloadDataObjectChunkFromResourceServer(session, taskID, conn, handle, localPath, blockReadCallback)
			if err != nil {
				dnErr := xerrors.Errorf("failed to download data object chunk %q from resource server: %w", irodsPath, err)
				errChan <- dnErr
			}
		}

		for i := 0; i < handle.Threads; i++ {
			taskWaitGroup.Add(1)

			go downloadTask(i)
		}

		taskWaitGroup.Wait()

		if len(errChan) > 0 {
			return handle.CheckSum, <-errChan
		}

		return handle.CheckSum, nil
	}

	return "", xerrors.Errorf("unhandled case, thread number is %d", handle.Threads)
}

// UploadDataObjectToResourceServer uploads a data object at the local path to the iRODS path
func UploadDataObjectToResourceServer(session *session.IRODSSession, localPath string, irodsPath string, resource string, taskNum int, replicate bool, keywords map[common.KeyWord]string, callback common.TrackerCallBack) error {
	logger := log.WithFields(log.Fields{
		"package":  "fs",
		"function": "UploadDataObjectToResourceServer",
	})

	logger.Debugf("upload data object %q", irodsPath)

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

	conn, err := session.AcquireConnection()
	if err != nil {
		return xerrors.Errorf("failed to get connection: %w", err)
	}

	if conn == nil || !conn.IsConnected() {
		session.ReturnConnection(conn)
		return xerrors.Errorf("connection is nil or disconnected")
	}

	handle, err := GetDataObjectRedirectionInfoForPut(conn, irodsPath, resource, fileLength, numTasks, keywords)
	if err != nil {
		logger.WithError(err).Debugf("failed to get redirection info for data object %q, switch to UploadDataObjctParallel", irodsPath)

		session.ReturnConnection(conn)
		return UploadDataObjectParallel(session, localPath, irodsPath, resource, 0, replicate, keywords, callback)
	}

	// we defer return connection here to not occupy connection when switched to UploadDataObjectParallel
	defer session.ReturnConnection(conn)

	defer CompleteDataObjectRedirection(conn, handle)

	if handle.Threads <= 0 || handle.RedirectionInfo == nil {
		// put file
		err = UploadDataObjectParallel(session, localPath, irodsPath, resource, 0, replicate, keywords, callback)
		if err != nil {
			return xerrors.Errorf("failed to upload data object %q to resource server: %w", localPath, err)
		}
		return nil
	} else if handle.RedirectionInfo != nil {
		logger.Debugf("Redirect to resource: path %q, threads %d, addr %q, port %d, window size %d, cookie %d", handle.Path, handle.Threads, handle.RedirectionInfo.Host, handle.RedirectionInfo.Port, handle.RedirectionInfo.WindowSize, handle.RedirectionInfo.Cookie)
		// put to portal
		errChan := make(chan error, handle.Threads)
		taskWaitGroup := sync.WaitGroup{}

		totalBytesUploaded := int64(0)
		if callback != nil {
			callback(totalBytesUploaded, fileLength)
		}

		// task progress
		taskProgress := make([]int64, handle.Threads)

		uploadTask := func(taskID int) {
			taskProgress[taskID] = 0

			defer taskWaitGroup.Done()

			blockWriteCallback := func(processed int64, total int64) {
				if processed > 0 {
					delta := processed - taskProgress[taskID]
					taskProgress[taskID] = processed

					atomic.AddInt64(&totalBytesUploaded, int64(delta))

					if callback != nil {
						callback(totalBytesUploaded, fileLength)
					}
				}
			}

			err = uploadDataObjectChunkToResourceServer(session, taskID, conn, handle, localPath, blockWriteCallback)
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

	return xerrors.Errorf("unhandled case, thread number is %d", handle.Threads)
}
