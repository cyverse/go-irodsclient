package metrics

import "sync"

// IRODSMetrics - contains IRODS metrics
type IRODSMetrics struct {
	// operations
	stat             uint64
	list             uint64
	search           uint64
	collectionCreate uint64
	collectionDelete uint64
	collectionRename uint64
	dataObjectCreate uint64
	dataObjectOpen   uint64
	dataObjectClose  uint64
	dataObjectDelete uint64
	dataObjectRename uint64
	dataObjectUpdate uint64
	dataObjectCopy   uint64
	dataObjectRead   uint64
	dataObjectWrite  uint64
	metadataList     uint64
	metadataCreate   uint64
	metadataDelete   uint64
	metadataUpdate   uint64
	accessList       uint64
	accessUpdate     uint64

	// transfer
	bytesSent     uint64 // done
	bytesReceived uint64 // done

	// cache
	cacheHit  uint64
	cacheMiss uint64

	// file handles - gauge
	openFileHandles uint64

	// connections - gauge
	connectionsOpened   uint64 // done
	connectionsOccupied uint64 // done

	// failures
	requestResponseFailures uint64 // done
	connectionFailures      uint64 // done
	connectionPoolFailures  uint64 // done

	mutex sync.Mutex
}

// IncreaseCounterForStat increases the counter for dataobject/collection stat
func (metrics *IRODSMetrics) IncreaseCounterForStat(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.stat += n
}

// GetCounterForStat returns the counter for dataobject/collection stat
func (metrics *IRODSMetrics) GetCounterForStat() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.stat
}

// IncreaseCounterForList increases the counter for listing
func (metrics *IRODSMetrics) IncreaseCounterForList(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.list += n
}

// GetCounterForList returns the counter for listing
func (metrics *IRODSMetrics) GetCounterForList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.list
}

// IncreaseCounterForSearch increases the counter for search
func (metrics *IRODSMetrics) IncreaseCounterForSearch(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.list += n
}

// GetCounterForSearch returns the counter for search
func (metrics *IRODSMetrics) GetCounterForSearch() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.list
}

// IncreaseCounterForCollectionCreate increases the counter for collection creation
func (metrics *IRODSMetrics) IncreaseCounterForCollectionCreate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.collectionCreate += n
}

// GetCounterForCollectionCreate returns the counter for collection creation
func (metrics *IRODSMetrics) GetCounterForCollectionCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.collectionCreate
}

// IncreaseCounterForCollectionDelete increases the counter for collection deletion
func (metrics *IRODSMetrics) IncreaseCounterForCollectionDelete(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.collectionDelete += n
}

// GetCounterForCollectionDelete returns the counter for collection deletion
func (metrics *IRODSMetrics) GetCounterForCollectionDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.collectionDelete
}

// IncreaseCounterForCollectionRename increases the counter for collection renameing
func (metrics *IRODSMetrics) IncreaseCounterForCollectionRename(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.collectionRename += n
}

// GetCounterForCollectionRename returns the counter for collection renameing
func (metrics *IRODSMetrics) GetCounterForCollectionRename() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.collectionRename
}

// IncreaseCounterForDataObjectCreate increases the counter for data object creation
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectCreate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectCreate += n
}

// GetCounterForDataObjectCreate returns the counter for data object creation
func (metrics *IRODSMetrics) GetCounterForDataObjectCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectCreate
}

// IncreaseCounterForDataObjectOpen increases the counter for data object opening
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectOpen(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectOpen += n
}

// GetCounterForDataObjectOpen returns the counter for data object opening
func (metrics *IRODSMetrics) GetCounterForDataObjectOpen() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectOpen
}

// IncreaseCounterForDataObjectClose increases the counter for data object closing
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectClose(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectClose += n
}

// GetCounterForDataObjectClose returns the counter for data object closing
func (metrics *IRODSMetrics) GetCounterForDataObjectClose() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectClose
}

// IncreaseCounterForDataObjectDelete increases the counter for data object deletion
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectDelete(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectDelete += n
}

// GetCounterForDataObjectDelete returns the counter for data object deletion
func (metrics *IRODSMetrics) GetCounterForDataObjectDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectDelete
}

// IncreaseCounterForDataObjectRename increases the counter for data object renaming
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectRename(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectRename += n
}

// GetCounterForDataObjectRename returns the counter for data object renaming
func (metrics *IRODSMetrics) GetCounterForDataObjectRename() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectRename
}

// IncreaseCounterForDataObjectCopy increases the counter for data object copy
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectCopy(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectCopy += n
}

// GetCounterForDataObjectCopy returns the counter for data object copy
func (metrics *IRODSMetrics) GetCounterForDataObjectCopy() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectCopy
}

// IncreaseCounterForDataObjectUpdate increases the counter for data object update (truncate, ETC)
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectUpdate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectUpdate += n
}

// GetCounterForDataObjectUpdate returns the counter for data object update (truncate, ETC)
func (metrics *IRODSMetrics) GetCounterForDataObjectUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectUpdate
}

// IncreaseCounterForDataObjectRead increases the counter for data object read
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectRead(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectRead += n
}

// GetCounterForDataObjectRead returns the counter for data object read
func (metrics *IRODSMetrics) GetCounterForDataObjectRead() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectRead
}

// IncreaseCounterForDataObjectWrite increases the counter for data object write
func (metrics *IRODSMetrics) IncreaseCounterForDataObjectWrite(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.dataObjectWrite += n
}

// GetCounterForDataObjectWrite returns the counter for data object write
func (metrics *IRODSMetrics) GetCounterForDataObjectWrite() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.dataObjectWrite
}

// IncreaseCounterForMetadataList increases the counter for metadata listing
func (metrics *IRODSMetrics) IncreaseCounterForMetadataList(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.metadataList += n
}

// GetCounterForMetadataList returns the counter for metadata listing
func (metrics *IRODSMetrics) GetCounterForMetadataList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.metadataList
}

// IncreaseCounterForMetadataCreate increases the counter for metadata creatation
func (metrics *IRODSMetrics) IncreaseCounterForMetadataCreate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.metadataCreate += n
}

// GetCounterForMetadataCreate returns the counter for metadata creatation
func (metrics *IRODSMetrics) GetCounterForMetadataCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.metadataCreate
}

// IncreaseCounterForMetadataDelete increases the counter for metadata deletion
func (metrics *IRODSMetrics) IncreaseCounterForMetadataDelete(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.metadataDelete += n
}

// GetCounterForMetadataDelete returns the counter for metadata deletion
func (metrics *IRODSMetrics) GetCounterForMetadataDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.metadataDelete
}

// IncreaseCounterForMetadataUpdate increases the counter for metadata update
func (metrics *IRODSMetrics) IncreaseCounterForMetadataUpdate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.metadataUpdate += n
}

// GetCounterForMetadataUpdate returns the counter for metadata update
func (metrics *IRODSMetrics) GetCounterForMetadataUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.metadataUpdate
}

// IncreaseCounterForAccessList increases the counter for dataobject/collection access listing
func (metrics *IRODSMetrics) IncreaseCounterForAccessList(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.accessList += n
}

// GetCounterForAccessList returns the counter for dataobject/collection access listing
func (metrics *IRODSMetrics) GetCounterForAccessList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.accessList
}

// IncreaseCounterForAccessUpdate increases the counter for dataobject/collection access update
func (metrics *IRODSMetrics) IncreaseCounterForAccessUpdate(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.accessUpdate += n
}

// GetCounterForAccessUpdate returns the counter for dataobject/collection access update
func (metrics *IRODSMetrics) GetCounterForAccessUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.accessUpdate
}

// IncreaseBytesSent increases bytes sent
func (metrics *IRODSMetrics) IncreaseBytesSent(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.bytesSent += n
}

// GetBytesSent returns bytes sent
func (metrics *IRODSMetrics) GetBytesSent() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.bytesSent
}

// IncreaseBytesReceived increases bytes received
func (metrics *IRODSMetrics) IncreaseBytesReceived(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.bytesReceived += n
}

// GetBytesReceived returns bytes received
func (metrics *IRODSMetrics) GetBytesReceived() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.bytesReceived
}

// IncreaseCounterForCacheHit increases the counter for cache hit
func (metrics *IRODSMetrics) IncreaseCounterForCacheHit(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.cacheHit += n
}

// GetCounterForCacheHit returns the counter for cache hit
func (metrics *IRODSMetrics) GetCounterForCacheHit() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.cacheHit
}

// IncreaseCounterForCacheMiss increases the counter for cache miss
func (metrics *IRODSMetrics) IncreaseCounterForCacheMiss(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.cacheMiss += n
}

// GetCounterForCacheMiss returns the counter for cache miss
func (metrics *IRODSMetrics) GetCounterForCacheMiss() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.cacheMiss
}

// IncreaseOpenFileHandles increases the counter for open file handles
func (metrics *IRODSMetrics) IncreaseCounterForOpenFileHandles(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.openFileHandles += n
}

// DecreaseOpenFileHandles decreases the counter for open file handles
func (metrics *IRODSMetrics) DecreaseCounterForOpenFileHandles(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	if metrics.openFileHandles < n {
		metrics.openFileHandles = 0
	} else {
		metrics.openFileHandles -= n
	}
}

// GetCounterForOpenFileHandles returns the counter for open file handles
func (metrics *IRODSMetrics) GetCounterForOpenFileHandles() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.openFileHandles
}

// IncreaseConnectionsOpened increases connections opened
func (metrics *IRODSMetrics) IncreaseConnectionsOpened(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.connectionsOpened += n
}

// DecreaseConnectionsOpened decreases connections opened
func (metrics *IRODSMetrics) DecreaseConnectionsOpened(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	if metrics.connectionsOpened < n {
		metrics.connectionsOpened = 0
	} else {
		metrics.connectionsOpened -= n
	}
}

// GetConnectionsOpened returns connections opened
func (metrics *IRODSMetrics) GetConnectionsOpened() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.connectionsOpened
}

// IncreaseConnectionsOccupied increases connections occupied
func (metrics *IRODSMetrics) IncreaseConnectionsOccupied(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.connectionsOccupied += n
}

// DecreaseConnectionsOccupied decreases connections occupied
func (metrics *IRODSMetrics) DecreaseConnectionsOccupied(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	if metrics.connectionsOccupied < n {
		metrics.connectionsOccupied = 0
	} else {
		metrics.connectionsOccupied -= n
	}
}

// GetConnectionsOccupied returns connections occupied
func (metrics *IRODSMetrics) GetConnectionsOccupied() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.connectionsOccupied
}

func (metrics *IRODSMetrics) ClearConnections() {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.connectionsOccupied = 0
	metrics.connectionsOpened = 0
}

// IncreaseCounterForRequestResponseFailures increases the counter for request-response failures
func (metrics *IRODSMetrics) IncreaseCounterForRequestResponseFailures(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.requestResponseFailures += n
}

// GetCounterForRequestResponseFailures returns the counter for request-response failures
func (metrics *IRODSMetrics) GetCounterForRequestResponseFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.requestResponseFailures
}

// IncreaseCounterForConnectionFailures increases the counter for connection failures
func (metrics *IRODSMetrics) IncreaseCounterForConnectionFailures(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.connectionFailures += n
}

// GetCounterForConnectionFailures returns the counter for connection failures
func (metrics *IRODSMetrics) GetCounterForConnectionFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.connectionFailures
}

// IncreaseCounterForConnectionPoolFailures increases the counter for connection pool failures
func (metrics *IRODSMetrics) IncreaseCounterForConnectionPoolFailures(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.connectionPoolFailures += n
}

// GetCounterForConnectionPoolFailures returns the counter for connection pool failures
func (metrics *IRODSMetrics) GetCounterForConnectionPoolFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.connectionPoolFailures
}
