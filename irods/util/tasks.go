package util

const (
	TransferTaskMinLength int64 = 4 * 1024 * 1024 // 4MB
	TransferTaskMaxNum    int   = 4
	TransferBlockSize     int64 = 1024 * 1024 // 1MB
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
