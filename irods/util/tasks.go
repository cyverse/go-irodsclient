package util

const (
	// TransferTaskMinLength is a minimum data length of a task for parallel data transfer
	TransferTaskMinLength int64 = 32 * 1024 * 1024 // 32MB
	// TransferTaskMaxNum is a maximum number of tasks for parallel data transfer
	TransferTaskMaxNum int = 4
	// TransferBlockSize is a block size of a task
	TransferBlockSize int64 = 1024 * 1024 // 1MB
)

// GetNumTasksForParallelTransfer returns the number transfer tasks to be used
func GetNumTasksForParallelTransfer(dataObjectLength int64) int {
	if dataObjectLength <= TransferTaskMinLength {
		return 1
	}

	numTasks := int(dataObjectLength / TransferTaskMinLength)
	if dataObjectLength%TransferTaskMinLength > 0 {
		numTasks++
	}

	if numTasks <= 1 {
		return 1
	} else if numTasks > TransferTaskMaxNum {
		// too many tasks
		return TransferTaskMaxNum
	}

	return numTasks
}

// GetBlockSizeForParallelTransfer returns the block size
func GetBlockSizeForParallelTransfer(dataObjectLength int64) int64 {
	return TransferBlockSize
}
