# iRODS 4.3.3 + Redis Test Environment

Complete testing environment with iRODS 4.3.3 and Redis cache backend.

## Quick Start

### 1. Start iRODS + Redis

```bash
cd test/server/irods_4.3.3_redis
docker-compose up -d
```

### 2. Wait for Services to Be Ready

```bash
# Check status
docker-compose ps

# iRODS should show "healthy"
# Redis should be running
```

### 3. Verify Connectivity

```bash
# Test iRODS
iinit  # Login to iRODS

# Test Redis
redis-cli ping
# Output: PONG

# Redis UI: http://localhost:8081
```

## Services

| Service | Port | Container | Purpose |
|---------|------|-----------|---------|
| iRODS | 1247 | irods-catalog-provider | Main iRODS server |
| Data Transfer | 20000-20199 | irods-catalog-provider | Data transfer ports |
| Redis | 6379 | redis | Cache backend |
| Redis Commander | 8081 | redis-commander | Redis UI |

## Running Tests

### Run with Redis Cache Backend

```bash
# Set cache backend to redis
export CACHE_BACKEND=redis
export IRODS_HOST=localhost
export IRODS_PORT=1247

# Run tests
cd ../..
go test ./test/testcases -run TestLocalMain -v
```

### Or in Go Code

```go
config := &fs.CacheBackendConfig{
    Type: fs.CacheBackendTypeRedis,
    Redis: &fs.RedisBackendConfig{
        Address:        "localhost:6379",
        DB:             0,
        PoolSize:       10,
        ConnectTimeout: 5 * time.Second,
        CommandTimeout: 3 * time.Second,
        DefaultTTL:     1 * time.Minute,
    },
}

factory := fs.NewCacheBackendFactory(config)
backend, err := factory.CreateBackend()
if err != nil {
    log.Fatal(err)
}
defer backend.Close()

// Use with FileSystem
fsConfig := &fs.FileSystemConfig{
    Cache: &fs.CacheConfig{
        Backend: config,
    },
}

filesystem, err := fs.NewFileSystem(account, fsConfig)
```

## Configuration

### Environment Variables

```bash
# iRODS connection
export IRODS_HOST=localhost
export IRODS_PORT=1247
export IRODS_ZONE=tempZone
export IRODS_USER=rods
export IRODS_PASSWORD=rods

# Cache backend
export CACHE_BACKEND=redis  # or memory, ristretto
```

### YAML Configuration

```yaml
# config.yaml
file_system:
  cache:
    backend:
      type: "redis"
      default_ttl: "1m"
      redis:
        address: "localhost:6379"
        db: 0
        password: ""
        pool_size: 10
        connect_timeout: "5s"
        command_timeout: "3s"
```

## Docker Compose Commands

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# Stop and remove volumes (clears Redis data)
docker-compose down -v

# View logs
docker-compose logs -f irods-catalog-provider
docker-compose logs -f redis

# Restart a service
docker-compose restart redis
docker-compose restart irods-catalog-provider
```

## Debugging

### Check iRODS Status

```bash
docker exec irods-catalog-provider su - irods -c "./irodsctl status"
```

### Check Redis Data

```bash
# Connect to Redis CLI
redis-cli

# Inside Redis:
> KEYS *                           # List all keys
> GET irods:cache:entry:*         # Get cache entries
> DBSIZE                           # Show number of keys
> FLUSHDB                          # Clear all data
```

### View Network

```bash
# Check network
docker network inspect irods-redis-test-docker-4-3-3_irods-redis
```

## Comparison: Memory vs Ristretto vs Redis

```bash
# Test with Memory backend (single-instance only)
CACHE_BACKEND=memory go test ./test/testcases -run TestLocalMain -v

# Test with Ristretto backend (high-performance)
CACHE_BACKEND=ristretto go test ./test/testcases -run TestLocalMain -v

# Test with Redis backend (distributed)
CACHE_BACKEND=redis go test ./test/testcases -run TestLocalMain -v
```

## Account Isolation Testing

With Redis backend, verify account isolation:

```bash
# Create two separate FileSystem instances
account1 := &types.IRODSAccount{
    ClientUser: "rods",
    ClientZone: "tempZone",
    // ...
}

account2 := &types.IRODSAccount{
    ClientUser: "admin",
    ClientZone: "tempZone",
    // ...
}

fs1 := NewFileSystem(account1, config)
fs2 := NewFileSystem(account2, config)

// Data from fs1 should not be visible in fs2
// (due to account-based namespace isolation)
```

## Performance Testing

### Monitor Redis Performance

```bash
# Connect to Redis
redis-cli

# Inside Redis:
# Monitor all commands in real-time
> MONITOR

# Check stats
> INFO stats
> INFO memory
> INFO clients
```

### Profile Cache Operations

```bash
# Enable tracing in tests
CACHE_DEBUG=true go test ./test/testcases -run TestLocalMain -v
```

## Troubleshooting

### iRODS Failed to Start

```bash
# Check logs
docker-compose logs irods-catalog

# Restart
docker-compose restart irods-catalog-provider

# Check network
docker network inspect irods-redis-test-docker-4-3-3_irods-redis
```

### Redis Connection Refused

```bash
# Check if running
docker-compose ps redis

# Check logs
docker-compose logs redis

# Restart
docker-compose restart redis
```

### Port Already in Use

```bash
# Find process using port
lsof -i :1247   # iRODS
lsof -i :6379   # Redis
lsof -i :8081   # Redis Commander

# Kill process or change docker-compose.yml ports
```

### Clear All Data

```bash
# Remove containers and volumes
docker-compose down -v

# Start fresh
docker-compose up -d
```

## Related Documentation

- [iRODS Documentation](https://docs.irods.org/)
- [Redis Documentation](https://redis.io/documentation)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [CACHE_SYSTEM.md](../../CACHE_SYSTEM.md) - Cache architecture
- [CACHING.md](../../CACHING.md) - Caching usage guide

## Notes

- iRODS default credentials: `rods/rods`
- iRODS zone: `tempZone`
- Redis has no password (local testing only)
- All services on same Docker network: `irods-redis-test-docker-4-3-3_irods-redis`
- Data persisted to Redis volume `redis-data`
