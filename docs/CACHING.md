# Caching Architecture and Consistency

## Overview

The go-irodsclient library uses a multi-level caching strategy to improve performance by reducing redundant iRODS server queries. However, in certain scenarios—particularly in service-oriented architectures with separate FileSystem instances per request—cache consistency issues can arise.

## Cache Types

The FileSystem maintains several independent caches:

1. **Entry Cache** — Individual file/collection metadata
2. **Negative Entry Cache** — Records paths that don't exist (fail-fast on repeated misses)
3. **Directory Cache** — Lists of entries in collections (for List operations)
4. **Metadata Cache** — Custom metadata (AVUs)
5. **ACL Cache** — Access control lists
6. **User/Group/Ticket Caches** — User and group information

## Default Cache Behavior

- **Cache TTL**: 1 minute (configurable via `CacheConfig.Timeout`)
- **Invalidation**: Occurs automatically on mutations within the same FileSystem instance
- **Propagation**: Events broadcast to other live FileSystem instances
- **New instances**: Start with empty caches; may observe stale state if server hasn't fully propagated changes

## Consistency Modes

### 1. Eventual Consistency (Default)

The default behavior is **eventual consistency** optimized for single-session use or when cache reuse is more valuable than strict post-mutation visibility.

```go
// Within same FileSystem instance
filesystem.MakeDir("/path/to/dir", false)
entry, _ := filesystem.StatDir("/path/to/dir")  // Guaranteed fresh
```

**However**, across separate FileSystem instances or requests:

```go
// Request A
filesystem1.MakeDir("/path/to/dir", false)
// ... returns success

// Request B (new FileSystem instance)
exists := filesystem2.ExistsDir("/path/to/dir")  // May be false briefly
// Stale state possible until cache expires or server fully propagates
```

### 2. Strong Consistency (Fresh-Read Methods)

Use explicit **fresh-read** method variants for strong consistency requirements. These always bypass cache and fetch from the iRODS server.

```go
// Fresh-read methods always query the server
entry, err := filesystem.StatFresh(path)
exists := filesystem.ExistsFresh(path)
entries, err := filesystem.ListFresh(path)
```

## Fresh-Read Methods

When you need guaranteed fresh state from the iRODS server, use the `Fresh` variants:

### Path Operations

- **`Stat(path)` vs `StatFresh(path)`** — Get entry status
- **`StatDir(path)` vs `StatDirFresh(path)`** — Get directory status
- **`StatFile(path)` vs `StatFileFresh(path)`** — Get file status
- **`Exists(path)` vs `ExistsFresh(path)`** — Check if exists
- **`ExistsDir(path)` vs `ExistsDirFresh(path)`** — Check if dir exists
- **`ExistsFile(path)` vs `ExistsFileFresh(path)`** — Check if file exists
- **`List(path)` vs `ListFresh(path)`** — List collection contents

### When to Use Fresh-Read Methods

**Use fresh-read methods when:**

1. **Cross-request consistency** — Reading in a different HTTP request or session after a mutation
   ```go
   // POST /api/collections (creates a collection)
   fs1.MakeDir("/path", false)
   
   // GET /api/collections/{name} (reads in new request)
   exists := fs2.ExistsFresh("/path")  // Use fresh read
   ```

2. **Multi-FileSystem patterns** — Service layers that create one FileSystem per request
   ```go
   // Service pattern: one fs per request
   fs := getFileSystemForRequest()
   defer fs.Release()
   
   err := fs.RemoveFile(path, false)
   if err == nil {
       // After mutation, read in same fs is cached (OK)
       // But for a follow-up request, use fresh-read
   }
   ```

3. **Polling or retry loops** — After mutations, waiting for changes to become visible
   ```go
   err := fs.MakeDir(path, false)
   if err == nil {
       for i := 0; i < maxRetries; i++ {
           if fs.ExistsFresh(path) {  // Fresh read each time
               break
           }
           time.Sleep(100 * time.Millisecond)
       }
   }
   ```

### When Regular Methods Are Fine

**Use regular (cached) methods when:**

- Reading within the same FileSystem instance after a mutation
- Repeated reads of the same path (performance matters more than millisecond freshness)
- Batch operations on the same FileSystem (cache reuse is valuable)
- Consistency latency up to 1 minute is acceptable

## Cache Configuration

### Disable Caching Globally

To disable caching entirely (strong consistency everywhere, but slower):

```go
config := fs.NewFileSystemConfig("myapp")
config.Cache.NoCache = true
filesystem, _ := fs.NewFileSystem(account, config)
```

### Per-Path TTL Settings

Configure different cache timeouts for specific paths:

```go
config := fs.NewFileSystemConfig("myapp")
config.Cache.MetadataTimeoutSettings = []fs.MetadataCacheTimeoutSetting{
    {
        Path: "/path/to/hot",
        Timeout: types.Duration(5 * time.Minute),  // Longer TTL
        Inherit: true,
    },
    {
        Path: "/path/to/volatile",
        Timeout: types.Duration(5 * time.Second),  // Shorter TTL
        Inherit: false,
    },
}
filesystem, _ := fs.NewFileSystem(account, config)
```

### Parent Directory Invalidation

Control whether parent directory caches are invalidated on child operations:

```go
config := fs.NewFileSystemConfig("myapp")
config.Cache.InvalidateParentEntryCacheImmediately = true  // Default: true
// When false, short inconsistency windows allowed
```

## Propagation Between Instances

When FileSystem instances are active in the same process, cache invalidation events propagate automatically:

```go
fs1, _ := fs.NewFileSystem(account, config)
fs2, _ := fs.NewFileSystem(account, config)

// Mutation in fs1
fs1.MakeDir("/path", false)

// fs2 receives invalidation event (async)
// fs2.cache for /path is cleared
// Next read in fs2 will fetch fresh state
```

**Limitation**: This only works for FileSystem instances in the same process. Separate services/processes don't share this propagation mechanism.

## Performance vs Consistency Trade-offs

| Scenario | Method | Latency | Consistency |
|----------|--------|---------|-------------|
| Single fs, read after write | `Stat()` | <1ms (cached) | Strong |
| Cross-request, strong requirement | `StatFresh()` | 5-50ms | Strong |
| Batch reads, repeated paths | `Stat()` | <1ms | Eventual (~60s) |
| Service endpoint, hot path | `Stat()` | <1ms | Eventual (~60s) |
| Service endpoint, post-mutation check | `StatFresh()` | 5-50ms | Strong |

## Debugging Cache Behavior

### Enable Debug Logging

Set the log level to trace cache hits/misses:

```go
log.SetLevel(log.DebugLevel)
```

### Check Cache Stats

Monitor cache behavior via filesystem metrics:

```go
metrics := filesystem.GetMetrics()
// Inspect metrics for cache effectiveness
```

## Recommendations

1. **Default**: Use regular methods (`Stat()`, `List()`, etc.) for performance
2. **Service layers**: Use `Fresh` variants after mutations that need immediate visibility
3. **Polling loops**: Use `Fresh` variants in retry logic after mutations
4. **Critical paths**: Consider reducing `Cache.Timeout` for volatile paths
5. **New instances**: If creating a fresh FileSystem for a single operation, cache TTL may not matter—consider fresh-read methods

## Related Issues

- Cross-session cache inconsistency: When separate FileSystem instances don't see mutations immediately
- Negative cache persistence: When a failed lookup is cached, hiding late arrivals
- TTL mismatch: When different code assumes different cache visibility windows

## Examples

### Example 1: REST API Pattern

```go
// POST /api/create-collection
func CreateCollection(w http.ResponseWriter, r *http.Request) {
    fs := getFileSystemForRequest()
    defer fs.Release()
    
    path := r.FormValue("path")
    
    err := fs.MakeDir(path, false)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
    
    // Return success with fresh verification
    exists := fs.ExistsFresh(path)
    if exists {
        w.WriteHeader(http.StatusCreated)
    } else {
        w.WriteHeader(http.StatusInternalServerError)
    }
}
```

### Example 2: Batch Operations with Polling

```go
// Create multiple collections, wait for visibility
for _, path := range paths {
    fs.MakeDir(path, false)
}

// Verify all are visible
for _, path := range paths {
    for i := 0; i < 5; i++ {
        if fs.ExistsFresh(path) {
            break
        }
        time.Sleep(100 * time.Millisecond)
    }
}
```

### Example 3: Consistent Listing After Modification

```go
// Add file to collection
fs.CreateFile(filePath, "", "w")

// List parent with fresh state
entries, err := fs.ListFresh(parentPath)
// All recent additions are guaranteed visible
```
