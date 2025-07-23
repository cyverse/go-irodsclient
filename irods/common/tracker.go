package common

type TransferTrackerCallback func(taskName string, processed int64, total int64)
