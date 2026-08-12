package common

type TransferTrackerCallback func(taskName string, processed int64, total int64)

// DataObjectBlockCallback is called for each block of data read during download.
// The data slice is backed by a pooled buffer and is only valid during the callback invocation.
// Copy the data if you need to retain it beyond the callback return.
type DataObjectBlockCallback func(data []byte, offset int64) error
