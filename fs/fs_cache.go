package fs

import (
	"github.com/cyverse/go-irodsclient/irods/util"
)

// ClearCache clears all file system caches
func (fs *FileSystem) ClearCache() {
	fs.cache.ClearACLsCache()
	fs.cache.ClearMetadataCache()
	fs.cache.ClearEntryCache()
	fs.cache.ClearNegativeEntryCache()
	fs.cache.ClearDirCache()
}

// AddCacheEventHandler adds cache event handler
func (fs *FileSystem) AddCacheEventHandler(handler FilesystemCacheEventHandler) string {
	return fs.cacheEventHandlerMap.AddEventHandler(handler)
}

// RemoveCacheEventHandler removes cache event handler
func (fs *FileSystem) RemoveCacheEventHandler(handlerID string) {
	fs.cacheEventHandlerMap.RemoveEventHandler(handlerID)
}

// invalidateCacheForRemoveInternal invalidates cache for removal of the given file/dir
func (fs *FileSystem) invalidateCacheForRemoveInternal(path string, recurse bool) {
	var entry *Entry
	if recurse {
		entry = fs.cache.GetEntryCache(path)
	}

	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveACLsCache(path)
	fs.cache.RemoveMetadataCache(path)

	if recurse && entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCacheForRemoveInternal(dirEntry, recurse)
			}
		}
	}

	// remove dircache and dir acl cache even if it is a file or unknown, no harm.
	fs.cache.RemoveDirCache(path)
}

// invalidateCacheForDirCreate invalidates cache for creation of the given dir
func (fs *FileSystem) invalidateCacheForDirCreate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		parentDirEntries = append(parentDirEntries, path)
		fs.cache.AddDirCache(parentPath, parentDirEntries)
	}

	// send event
	fs.cacheEventHandlerMap.SendDirCreateEvent(path)
}

// invalidateCacheForDirUpdate invalidates cache for update of the given dir
func (fs *FileSystem) invalidateCacheForDirUpdate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)

	// modification doesn't affect to parent dir's modified time

	// send event
	fs.cacheEventHandlerMap.SendDirCreateEvent(path)
}

// invalidateCacheForFileUpdate invalidates cache for update on the given file
func (fs *FileSystem) invalidateCacheForFileUpdate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)

	// modification doesn't affect to parent dir's modified time

	// send event
	fs.cacheEventHandlerMap.SendFileUpdateEvent(path)
}

// invalidateCacheForDirRemove invalidates cache for removal of the given dir
func (fs *FileSystem) invalidateCacheForDirRemove(path string, recurse bool) {
	var entry *Entry
	if recurse {
		entry = fs.cache.GetEntryCache(path)
	}

	// we need to expunge all negatie entry caches under irodsDestPath
	// since all sub-directories/files are also moved
	fs.cache.RemoveAllNegativeEntryCacheForPath(path)

	fs.cache.AddNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveMetadataCache(path)

	if recurse && entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCacheForRemoveInternal(dirEntry, recurse)
			}
		}
	}

	fs.cache.RemoveDirCache(path)
	fs.cache.RemoveACLsCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		newParentDirEntries := []string{}
		for _, dirEntry := range parentDirEntries {
			if dirEntry != path {
				newParentDirEntries = append(newParentDirEntries, dirEntry)
			}
		}
		fs.cache.AddDirCache(parentPath, newParentDirEntries)
	}

	// send event
	fs.cacheEventHandlerMap.SendDirRemoveEvent(path)
}

// invalidateCacheForDirExtract invalidates cache for bundle extraction of the given dir
// this occurs when extracting bundle files
func (fs *FileSystem) invalidateCacheForDirExtract(path string) {
	entry := fs.cache.GetEntryCache(path)

	// we need to expunge all negatie entry caches under irodsDestPath
	// since it also creates sub-directories/files
	fs.cache.RemoveAllNegativeEntryCacheForPath(path)

	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveMetadataCache(path)

	if entry != nil {
		if entry.Type == DirectoryEntry {
			dirEntries := fs.cache.GetDirCache(path)
			for _, dirEntry := range dirEntries {
				// do it recursively
				fs.invalidateCacheForRemoveInternal(dirEntry, true)
			}
		}
	}

	fs.cache.RemoveDirCache(path)
	fs.cache.RemoveACLsCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		exist := false
		for _, parentDirEntry := range parentDirEntries {
			if parentDirEntry == path {
				exist = true
				break
			}
		}

		if !exist {
			parentDirEntries = append(parentDirEntries, path)
		}

		fs.cache.AddDirCache(parentPath, parentDirEntries)
	}

	// send event
	fs.cacheEventHandlerMap.SendDirCreateEvent(path)
}

// invalidateCacheForFileCreate invalidates cache for creation of the given file
func (fs *FileSystem) invalidateCacheForFileCreate(path string) {
	fs.cache.RemoveNegativeEntryCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		parentDirEntries = append(parentDirEntries, path)
		fs.cache.AddDirCache(parentPath, parentDirEntries)
	}

	// send event
	fs.cacheEventHandlerMap.SendFileCreateEvent(path)
}

// invalidateCacheForFileRemove invalidates cache for removal of the given file
func (fs *FileSystem) invalidateCacheForFileRemove(path string) {
	fs.cache.AddNegativeEntryCache(path)
	fs.cache.RemoveEntryCache(path)
	fs.cache.RemoveACLsCache(path)
	fs.cache.RemoveMetadataCache(path)

	// parent dir's entry also changes
	fs.cache.RemoveParentDirCache(path)
	// parent dir's dir entry also changes
	parentPath := util.GetIRODSPathDirname(path)
	parentDirEntries := fs.cache.GetDirCache(parentPath)
	if parentDirEntries != nil {
		newParentDirEntries := []string{}
		for _, dirEntry := range parentDirEntries {
			if dirEntry != path {
				newParentDirEntries = append(newParentDirEntries, dirEntry)
			}
		}
		fs.cache.AddDirCache(parentPath, newParentDirEntries)
	}

	// send event
	fs.cacheEventHandlerMap.SendFileRemoveEvent(path)
}
