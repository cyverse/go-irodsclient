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
- **Propagation**: 
  - **Same user, same process**: Shared cache (CacheManager) → mutations immediately visible
  - **Different user or process**: May observe stale state until cache expires
- **Cache sharing**: Multiple FileSystem instances for the same user (same account/host/zone) now share a cache via CacheManager

## Consistency Modes

### 1. Strong Consistency (Default for Same User)

When multiple FileSystem instances are created for the **same user** (account/host/zone), they now share a cache via CacheManager. This provides **strong consistency** automatically:

```go
// Request A
filesystem1.MakeDir("/path/to/dir", false)
// ... returns success

// Request B (same user, new FileSystem instance)
exists := filesystem2.ExistsDir("/path/to/dir")  // GUARANTEED true
// filesystem1 and filesystem2 share the same cache!
```

### 2. Eventual Consistency (Different Users or Processes)

Across different users or separate processes, the default behavior is **eventual consistency**:

```go
// Request A (user1)
filesystem1.MakeDir("/path/to/dir", false)
// ... returns success

// Request B (user2, or different process)
exists := filesystem2.ExistsDir("/path/to/dir")  // May be false briefly
// Stale state possible until cache expires or server fully propagates
```

**Within same FileSystem instance:**
```go
// Same instance: always fresh after mutation
filesystem.MakeDir("/path/to/dir", false)
entry, _ := filesystem.StatDir("/path/to/dir")  // Guaranteed fresh (invalidated in same cache)
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

When you need guaranteed fresh state from the iRODS server, use the `Fresh` variants. These always bypass cache and fetch directly from iRODS.

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

1. **Cross-user or cross-process scenarios** — Different FileSystem instances with different users/accounts
   ```go
   // User A's request
   fs_a.MakeDir("/path", false)
   
   // User B's request (different user/account)
   exists := fs_b.ExistsFresh("/path")  // Use fresh read
   // fs_a and fs_b have separate caches (different accountIDs)
   ```

2. **Polling or retry loops** — After mutations, waiting for iRODS server propagation
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

3. **Inter-process visibility** — When separate service processes modify the same path
   ```go
   // Process A
   fs_a.CreateFile("/path/file.txt", "", "w")
   
   // Process B (different process, different cache manager)
   exists := fs_b.ExistsFresh("/path/file.txt")  // Use fresh read
   // Process B doesn't share cache with Process A
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

## Cache Sharing Between Instances

### Same User (Same Account/Host/Zone)

When FileSystem instances are created for the same user in the same process, they automatically **share the same cache** via CacheManager:

```go
account := &types.IRODSAccount{
    Host: "irod.example.com",
    Port: 1247,
    ClientUser: "rods",
    ClientZone: "tempZone",
}

fs1, _ := fs.NewFileSystem(account, config)
fs2, _ := fs.NewFileSystem(account, config)

// Mutation in fs1
fs1.MakeDir("/path", false)

// fs2 sees it IMMEDIATELY (same cache object!)
exists := fs2.Exists("/path")  // true (no fresh-read needed)
```

**How it works:**
- Both instances generate same `accountID` from (host, port, user, zone)
- `GetCacheManager().AcquireCache()` returns same cache for same `accountID`
- Mutations in one instance immediately visible in the other
- Reference counting cleans up cache when all instances released

### Different Users or Different Hosts

When FileSystem instances have different accounts/hosts/zones, they have separate caches:

```go
accountA := &types.IRODSAccount{...user1...}
accountB := &types.IRODSAccount{...user2...}

fs1, _ := fs.NewFileSystem(accountA, config)
fs2, _ := fs.NewFileSystem(accountB, config)

// fs1 and fs2 have separate caches (different accountIDs)
// Mutations in one may not be visible in the other
```

### Cross-Process Isolation

CacheManager is process-local (not shared across processes):

```go
// Process A: creates FileSystem
Process_A: fs1.MakeDir("/path", false)
Process_A: cache has /path

// Process B: separate cache manager
Process_B: exists := fs2.Exists("/path")  // May be false (different cache)
Process_B: should use ExistsFresh() instead
```

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

1. **Same user, same process**: Use regular methods (`Stat()`, `List()`, etc.)
   - CacheManager ensures shared cache between instances
   - Mutations immediately visible via shared cache
   - No need for fresh-read methods

2. **Different user or cross-process**: Use `Fresh` variants after mutations
   - Different users have separate caches (different accountID)
   - Cross-process caches aren't shared
   - Fresh-read ensures server state visibility

3. **Polling/retry loops**: Use `Fresh` variants
   - Ensures iRODS server state, not cached state
   - Useful for waiting on server propagation

4. **Performance critical paths**: Use regular methods (cached)
   - CacheManager already provides same-user consistency
   - Cache reuse improves throughput

5. **Cache configuration**:
   - Default TTL (1 minute) is usually sufficient
   - Reduce TTL for high-volatility paths
   - Disable caching (`NoCache: true`) for strong consistency everywhere (slower)

## Related Issues and Solutions

### Cross-User Cache Inconsistency

**Issue**: Different FileSystem instances for different users have separate caches
**Solution**: Use `Fresh` variants when reading across different users/accounts
```go
fs_user1.MakeDir("/path", false)
exists = fs_user2.ExistsFresh("/path")  // Fresh read needed
```

### Negative Cache Persistence

**Issue**: Failed lookup is cached, hiding late arrivals
**Solution**: Use `Fresh` variants in polling loops
```go
if !fs.ExistsFresh(path) {  // Fresh read checks server
    time.Sleep(100 * time.Millisecond)
}
```

### TTL Mismatch

**Issue**: Different code assumes different cache visibility windows
**Solution**: Configure per-path TTL or disable cache for volatile paths
```go
config.Cache.MetadataTimeoutSettings = []fs.MetadataCacheTimeoutSetting{
    {Path: "/volatile/path", Timeout: types.Duration(5 * time.Second)},
}
```

### Same User, Multiple Instances

**Issue**: ~~Mutations not visible across instances~~ **SOLVED**
**Solution**: CacheManager automatically shares cache for same user
- Multiple FileSystem instances for same user share cache
- Mutations immediately visible (same cache object)
- No fresh-read workarounds needed for same-user scenarios

## Examples

### Example 1: REST API Pattern (Same User)

```go
// POST /api/create-collection
func CreateCollection(w http.ResponseWriter, r *http.Request) {
    fs := getFileSystemForRequest(userID)  // Same user throughout
    defer fs.Release()
    
    path := r.FormValue("path")
    
    err := fs.MakeDir(path, false)
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
    
    // Next request for same user will see the mutation
    // (CacheManager shares cache for same user/account)
    w.WriteHeader(http.StatusCreated)
}

// GET /api/collections/{name}
func GetCollection(w http.ResponseWriter, r *http.Request) {
    fs := getFileSystemForRequest(userID)  // Same user
    defer fs.Release()
    
    path := r.FormValue("path")
    
    // Regular Exists() is sufficient (shared cache)
    exists := fs.Exists(path)
    
    if exists {
        w.WriteHeader(http.StatusOK)
    } else {
        w.WriteHeader(http.StatusNotFound)
    }
}
```

### Example 1b: REST API Pattern (Different Users)

```go
// Cross-user scenario (admin listing user's files)
func AdminListUserFiles(w http.ResponseWriter, r *http.Request) {
    adminFS := getFileSystemForRequest(adminID)
    defer adminFS.Release()
    
    userID := r.FormValue("userID")
    userPath := fmt.Sprintf("/zone/home/%s", userID)
    
    // If separate user modified this path, need fresh read
    // because adminFS and userFS have separate caches
    entries, err := adminFS.ListFresh(userPath)
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }
    
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(entries)
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
