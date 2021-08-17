package fs

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cyverse/go-irodsclient/irods/common"
	"github.com/cyverse/go-irodsclient/irods/session"
	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/go-irodsclient/irods/util"
)

// UploadDataObject put a data object at the local path to the iRODS path
func UploadDataObject(session *session.IRODSSession, localPath string, irodsPath string, resource string, replicate bool) error {
	conn, err := session.AcquireConnection()
	if err != nil {
		return err
	}
	defer session.ReturnConnection(conn)

	if conn == nil || !conn.IsConnected() {
		return fmt.Errorf("connection is nil or disconnected")
	}

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	handle, err := OpenDataObjectWithOperation(conn, irodsPath, resource, "w", common.OPER_TYPE_PUT_DATA_OBJ)
	if err != nil {
		return err
	}

	// copy
	buffer := make([]byte, common.ReadWriteBufferSize)
	var writeErr error
	for {
		bytesRead, err := f.Read(buffer)
		if err != nil {
			CloseDataObject(conn, handle)
			if err == io.EOF {
				break
			} else {
				writeErr = err
				break
			}
		}

		err = WriteDataObject(conn, handle, buffer[:bytesRead])
		if err != nil {
			CloseDataObject(conn, handle)
			writeErr = err
			break
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

// DownloadDataObject downloads a data object at the iRODS path to the local path
func DownloadDataObject(session *session.IRODSSession, irodsPath string, resource string, localPath string) error {
	util.LogErrorf("download data object - %s\n", irodsPath)

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

	// copy
	for {
		buffer, err := ReadDataObject(conn, handle, common.ReadWriteBufferSize)
		if err != nil {
			return err
		}

		if len(buffer) == 0 {
			// EOF
			return nil
		} else {
			_, err = f.Write(buffer)
			if err != nil {
				return err
			}
		}
	}
}

// DownloadDataObjectParallel downloads a data object at the iRODS path to the local path in parallel
// Partitions a file into n (taskNum) tasks and downloads in parallel
func DownloadDataObjectParallel(session *session.IRODSSession, irodsPath string, resource string, localPath string, dataObjectLength int64, taskNum int) error {
	numTasks := taskNum
	if numTasks <= 0 {
		numTasks = util.GetNumTasksForParallelTransfer(dataObjectLength)
	}

	util.LogErrorf("download data object in parallel - %s, size(%d), threads(%d)\n", irodsPath, dataObjectLength, numTasks)

	// create an empty file
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	f.Close()

	errChan := make(chan error, numTasks)
	taskWaitGroup := sync.WaitGroup{}

	downloadTask := func(offset int64, length int64) {
		defer taskWaitGroup.Done()

		conn, err := session.AcquireConnection()
		if err != nil {
			errChan <- err
		}
		defer session.ReturnConnection(conn)

		if conn == nil || !conn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		handle, _, err := OpenDataObject(conn, irodsPath, resource, "r")
		if err != nil {
			errChan <- err
			return
		}

		defer CloseDataObject(conn, handle)

		f, err := os.OpenFile(localPath, os.O_WRONLY, 0)
		if err != nil {
			errChan <- err
			return
		}

		defer f.Close()

		newOffset, err := SeekDataObject(conn, handle, offset, types.SeekSet)
		if err != nil {
			errChan <- fmt.Errorf("could not seek a data object - %v", err)
			return
		}

		if newOffset != offset {
			errChan <- fmt.Errorf("could not seek to target offset %d", offset)
			return
		}

		remain := length

		// copy
		for remain > 0 {
			toCopy := remain
			if toCopy >= int64(common.ReadWriteBufferSize) {
				toCopy = int64(common.ReadWriteBufferSize)
			}

			buffer, err := ReadDataObject(conn, handle, int(toCopy))
			if err != nil {
				errChan <- err
				return
			}

			if len(buffer) == 0 {
				// EOF
				return
			} else {
				_, err = f.WriteAt(buffer, offset+(length-remain))
				if err != nil {
					errChan <- err
				}

				remain -= int64(len(buffer))
			}
		}
	}

	lengthPerThread := dataObjectLength / int64(numTasks)
	if dataObjectLength%int64(numTasks) > 0 {
		lengthPerThread++
	}

	offset := int64(0)
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

	util.LogErrorf("download data object in parallel - %s, size(%d), threads(%d), block_size(%d)\n", irodsPath, dataObjectLength, numTasks, blockSize)

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

		conn, err := session.AcquireConnection()
		if err != nil {
			errChan <- err
		}
		defer session.ReturnConnection(conn)

		if conn == nil || !conn.IsConnected() {
			errChan <- fmt.Errorf("connection is nil or disconnected")
			return
		}

		handle, _, err := OpenDataObject(conn, irodsPath, resource, "r")
		if err != nil {
			errChan <- err
			return
		}

		defer CloseDataObject(conn, handle)

		f, err := os.OpenFile(localPath, os.O_WRONLY, 0)
		if err != nil {
			errChan <- err
			return
		}

		defer f.Close()

		for {
			taskOffset, ok := <-inputChan
			if !ok {
				break
			}

			newOffset, err := SeekDataObject(conn, handle, taskOffset, types.SeekSet)
			if err != nil {
				errChan <- fmt.Errorf("could not seek a data object - %v", err)
				return
			}

			if newOffset != taskOffset {
				errChan <- fmt.Errorf("could not seek to target offset %d", taskOffset)
				return
			}

			remain := blockSize

			// copy
			for remain > 0 {
				toCopy := remain
				if toCopy >= int64(common.ReadWriteBufferSize) {
					toCopy = int64(common.ReadWriteBufferSize)
				}

				buffer, err := ReadDataObject(conn, handle, int(toCopy))
				if err != nil {
					errChan <- err
					return
				}

				if len(buffer) == 0 {
					// EOF
					break
				} else {
					_, err = f.WriteAt(buffer, taskOffset+(blockSize-remain))
					if err != nil {
						errChan <- err
					}

					remain -= int64(len(buffer))
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
