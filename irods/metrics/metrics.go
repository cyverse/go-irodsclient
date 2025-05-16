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
	bytesSent     uint64
	bytesReceived uint64

	// cache
	cacheHit  uint64
	cacheMiss uint64

	// file handles - gauge
	openFileHandles uint64

	// connections - gauge
	connectionsOpened   uint64
	connectionsOccupied uint64

	// failures
	requestResponseFailures uint64
	connectionFailures      uint64
	connectionPoolFailures  uint64

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

// GetAndClearCounterForStat returns the counter for dataobject/collection stat then clear
func (metrics *IRODSMetrics) GetAndClearCounterForStat() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	stat := metrics.stat
	metrics.stat = 0
	return stat
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

// GetAndClearCounterForList returns the counter for listing then clear
func (metrics *IRODSMetrics) GetAndClearCounterForList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	list := metrics.list
	metrics.list = 0
	return list
}

// IncreaseCounterForSearch increases the counter for search
func (metrics *IRODSMetrics) IncreaseCounterForSearch(n uint64) {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	metrics.search += n
}

// GetCounterForSearch returns the counter for search
func (metrics *IRODSMetrics) GetCounterForSearch() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	return metrics.search
}

// GetAndClearCounterForSearch returns the counter for search then clear
func (metrics *IRODSMetrics) GetAndClearCounterForSearch() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	search := metrics.search
	metrics.search = 0
	return search
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

// GetAndClearCounterForCollectionCreate returns the counter for collection creation then clear
func (metrics *IRODSMetrics) GetAndClearCounterForCollectionCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	create := metrics.collectionCreate
	metrics.collectionCreate = 0
	return create
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

// GetAndClearCounterForCollectionDelete returns the counter for collection deletion then clear
func (metrics *IRODSMetrics) GetAndClearCounterForCollectionDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	delete := metrics.collectionDelete
	metrics.collectionDelete = 0
	return delete
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

// GetAndClearCounterForCollectionRename returns the counter for collection renameing then clear
func (metrics *IRODSMetrics) GetAndClearCounterForCollectionRename() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	rename := metrics.collectionRename
	metrics.collectionRename = 0
	return rename
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

// GetAndClearCounterForDataObjectCreate returns the counter for data object creation then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	create := metrics.dataObjectCreate
	metrics.dataObjectCreate = 0
	return create
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

// GetAndClearCounterForDataObjectOpen returns the counter for data object opening then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectOpen() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	open := metrics.dataObjectOpen
	metrics.dataObjectOpen = 0
	return open
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

// GetAndClearCounterForDataObjectClose returns the counter for data object closing then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectClose() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	close := metrics.dataObjectClose
	metrics.dataObjectClose = 0
	return close
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

// GetAndClearCounterForDataObjectDelete returns the counter for data object deletion then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	delete := metrics.dataObjectDelete
	metrics.dataObjectDelete = 0
	return delete
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

// GetAndClearCounterForDataObjectRename returns the counter for data object renaming then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectRename() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	rename := metrics.dataObjectRename
	metrics.dataObjectRename = 0
	return rename
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

// GetAndClearCounterForDataObjectCopy returns the counter for data object copy then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectCopy() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	copy := metrics.dataObjectCopy
	metrics.dataObjectCopy = 0
	return copy
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

// GetAndClearCounterForDataObjectUpdate returns the counter for data object update (truncate, ETC) then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	update := metrics.dataObjectUpdate
	metrics.dataObjectUpdate = 0
	return update
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

// GetAndClearCounterForDataObjectRead returns the counter for data object read then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectRead() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	read := metrics.dataObjectRead
	metrics.dataObjectRead = 0
	return read
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

// GetAndClearCounterForDataObjectWrite returns the counter for data object write then clear
func (metrics *IRODSMetrics) GetAndClearCounterForDataObjectWrite() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	write := metrics.dataObjectWrite
	metrics.dataObjectWrite = 0
	return write
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

// GetAndClearCounterForMetadataList returns the counter for metadata listing then clear
func (metrics *IRODSMetrics) GetAndClearCounterForMetadataList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	list := metrics.metadataList
	metrics.metadataList = 0
	return list
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

// GetAndClearCounterForMetadataCreate returns the counter for metadata creatation then clear
func (metrics *IRODSMetrics) GetAndClearCounterForMetadataCreate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	create := metrics.metadataCreate
	metrics.metadataCreate = 0
	return create
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

// GetAndClearCounterForMetadataDelete returns the counter for metadata deletion then clear
func (metrics *IRODSMetrics) GetAndClearCounterForMetadataDelete() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	delete := metrics.metadataDelete
	metrics.metadataDelete = 0
	return delete
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

// GetAndClearCounterForMetadataUpdate returns the counter for metadata update then clear
func (metrics *IRODSMetrics) GetAndClearCounterForMetadataUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	update := metrics.metadataUpdate
	metrics.metadataUpdate = 0
	return update
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

// GetAndClearCounterForAccessList returns the counter for dataobject/collection access listing then clear
func (metrics *IRODSMetrics) GetAndClearCounterForAccessList() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	list := metrics.accessList
	metrics.accessList = 0
	return list
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

// GetAndClearCounterForAccessUpdate returns the counter for dataobject/collection access update then clear
func (metrics *IRODSMetrics) GetAndClearCounterForAccessUpdate() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	update := metrics.accessUpdate
	metrics.accessUpdate = 0
	return update
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

// GetAndClearBytesSent returns bytes sent then clear
func (metrics *IRODSMetrics) GetAndClearBytesSent() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	sent := metrics.bytesSent
	metrics.bytesSent = 0
	return sent
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

// GetAndClearBytesReceived returns bytes received then clear
func (metrics *IRODSMetrics) GetAndClearBytesReceived() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	received := metrics.bytesReceived
	metrics.bytesReceived = 0
	return received
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

// GetAndClearCounterForCacheHit returns the counter for cache hit then clear
func (metrics *IRODSMetrics) GetAndClearCounterForCacheHit() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	hit := metrics.cacheHit
	metrics.cacheHit = 0
	return hit
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

// GetAndClearCounterForCacheMiss returns the counter for cache miss then clear
func (metrics *IRODSMetrics) GetAndClearCounterForCacheMiss() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	miss := metrics.cacheMiss
	metrics.cacheMiss = 0
	return miss
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

// GetAndClearCounterForRequestResponseFailures returns the counter for request-response failures then clear
func (metrics *IRODSMetrics) GetAndClearCounterForRequestResponseFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	failures := metrics.requestResponseFailures
	metrics.requestResponseFailures = 0
	return failures
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

// GetAndClearCounterForConnectionFailures returns the counter for connection failures then clear
func (metrics *IRODSMetrics) GetAndClearCounterForConnectionFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	failures := metrics.connectionFailures
	metrics.connectionFailures = 0
	return failures
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

// GetAndClearCounterForConnectionPoolFailures returns the counter for connection pool failures then clear
func (metrics *IRODSMetrics) GetAndClearCounterForConnectionPoolFailures() uint64 {
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	failures := metrics.connectionPoolFailures
	metrics.connectionPoolFailures = 0
	return failures
}

func (metrics *IRODSMetrics) Sum(other *IRODSMetrics) {
	if other == nil {
		return
	}

	metrics.stat += other.stat
	metrics.list += other.list
	metrics.search += other.search
	metrics.collectionCreate += other.collectionCreate
	metrics.collectionDelete += other.collectionDelete
	metrics.collectionRename += other.collectionRename
	metrics.dataObjectCreate += other.dataObjectCreate
	metrics.dataObjectOpen += other.dataObjectOpen
	metrics.dataObjectClose += other.dataObjectClose
	metrics.dataObjectDelete += other.dataObjectDelete
	metrics.dataObjectRename += other.dataObjectRename
	metrics.dataObjectUpdate += other.dataObjectUpdate
	metrics.dataObjectCopy += other.dataObjectCopy
	metrics.dataObjectRead += other.dataObjectRead
	metrics.dataObjectWrite += other.dataObjectWrite
	metrics.metadataList += other.metadataList
	metrics.metadataCreate += other.metadataCreate
	metrics.metadataDelete += other.metadataDelete
	metrics.metadataUpdate += other.metadataUpdate
	metrics.accessList += other.accessList
	metrics.accessUpdate += other.accessUpdate
	metrics.bytesSent += other.bytesSent
	metrics.bytesReceived += other.bytesReceived
	metrics.cacheHit += other.cacheHit
	metrics.cacheMiss += other.cacheMiss
	metrics.openFileHandles += other.openFileHandles
	metrics.connectionsOpened += other.connectionsOpened
	metrics.connectionsOccupied += other.connectionsOccupied
	metrics.requestResponseFailures += other.requestResponseFailures
	metrics.connectionFailures += other.connectionFailures
	metrics.connectionPoolFailures += other.connectionPoolFailures
}
