package types

// CollectionIOMetrics - represents collection IO for access
type CollectionIOMetrics struct {
	Stat   uint64
	List   uint64
	Delete uint64
	Create uint64
	Rename uint64
	Meta   uint64
}

// DataObjectIOMetrics - represents data object IO for access
type DataObjectIOMetrics struct {
	Stat   uint64
	Read   uint64
	Write  uint64
	Delete uint64
	Create uint64
	Rename uint64
	Meta   uint64
}

// TransferMetrics - represents bytes transferred for access
type TransferMetrics struct {
	BytesReceived uint64
	BytesSent     uint64
	CollectionIO  CollectionIOMetrics
	DataObjectIO  DataObjectIOMetrics
}
