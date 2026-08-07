package fs

import (
	"testing"
	"time"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/stretchr/testify/assert"
)

func TestCacheManagerAcquireAndRelease(t *testing.T) {
	manager := GetCacheManager()
	defer manager.Clear()

	accountID := "test_account_1"
	config := NewDefaultCacheConfig()

	// Acquire cache first time
	cache1 := manager.AcquireCache(&config, accountID)
	assert.NotNil(t, cache1)

	stats := manager.GetStats()
	assert.Equal(t, 1, stats[accountID])

	// Acquire cache second time with same accountID
	cache2 := manager.AcquireCache(&config, accountID)
	assert.NotNil(t, cache2)

	// Should be the same cache instance
	assert.Equal(t, cache1, cache2)

	stats = manager.GetStats()
	assert.Equal(t, 2, stats[accountID])

	// Release first reference
	manager.ReleaseCache(accountID)
	stats = manager.GetStats()
	assert.Equal(t, 1, stats[accountID])

	// Cache should still exist because there's still one reference
	cache3 := manager.AcquireCache(&config, accountID)
	assert.Equal(t, cache1, cache3)
	stats = manager.GetStats()
	assert.Equal(t, 2, stats[accountID])

	// Release all references
	manager.ReleaseCache(accountID)
	stats = manager.GetStats()
	assert.Equal(t, 1, stats[accountID])

	manager.ReleaseCache(accountID)
	stats = manager.GetStats()
	// Cache should be removed when all references are released
	_, ok := stats[accountID]
	assert.False(t, ok)
}

func TestCacheManagerMultipleAccounts(t *testing.T) {
	manager := GetCacheManager()
	defer manager.Clear()

	config := NewDefaultCacheConfig()

	// Acquire caches for different accounts
	cache1 := manager.AcquireCache(&config, "account_1")
	assert.NotNil(t, cache1)

	cache2 := manager.AcquireCache(&config, "account_2")
	assert.NotNil(t, cache2)

	// Should be different cache instances
	assert.NotEqual(t, cache1, cache2)

	stats := manager.GetStats()
	assert.Equal(t, 1, stats["account_1"])
	assert.Equal(t, 1, stats["account_2"])

	manager.ReleaseCache("account_1")
	manager.ReleaseCache("account_2")

	stats = manager.GetStats()
	assert.Empty(t, stats)
}

func TestCacheManagerCachePropagation(t *testing.T) {
	manager := GetCacheManager()
	defer manager.Clear()

	config := NewDefaultCacheConfig()
	accountID := "test_account_shared"

	// Acquire first cache
	cache1 := manager.AcquireCache(&config, accountID)

	// Add entry to cache1
	now := time.Now()
	entry := &Entry{
		ID:                123,
		Type:              DirectoryEntry,
		Name:              "path",
		Path:              "/test/path",
		Owner:             "testuser",
		Size:              0,
		DataType:          "",
		CreateTime:        now,
		ModifyTime:        now,
		AccessTime:        now,
		CheckSumAlgorithm: types.ChecksumAlgorithmUnknown,
		CheckSum:          nil,
		IRODSReplicas:     nil,
		CacheID:           "test_cache_id",
	}
	cache1.AddEntryCache(entry)

	// Acquire second cache with same accountID
	cache2 := manager.AcquireCache(&config, accountID)

	// Should retrieve the same entry from the shared cache
	retrievedEntry := cache2.GetEntryCache("/test/path")
	assert.NotNil(t, retrievedEntry)
	assert.Equal(t, entry.ID, retrievedEntry.ID)

	manager.ReleaseCache(accountID)
	manager.ReleaseCache(accountID)
}

func TestCacheManagerGenerateAccountID(t *testing.T) {
	account1 := &types.IRODSAccount{
		Host:       "localhost",
		Port:       1247,
		ClientUser: "user1",
		ClientZone: "zone1",
	}

	account2 := &types.IRODSAccount{
		Host:       "localhost",
		Port:       1247,
		ClientUser: "user1",
		ClientZone: "zone1",
	}

	account3 := &types.IRODSAccount{
		Host:       "localhost",
		Port:       1247,
		ClientUser: "user2",
		ClientZone: "zone1",
	}

	accountID1 := GenerateAccountID(account1.Host, account1.Port, account1.ClientUser, account1.ClientZone)
	accountID2 := GenerateAccountID(account2.Host, account2.Port, account2.ClientUser, account2.ClientZone)
	accountID3 := GenerateAccountID(account3.Host, account3.Port, account3.ClientUser, account3.ClientZone)

	// Same account details should produce same ID
	assert.Equal(t, accountID1, accountID2)

	// Different account details should produce different ID
	assert.NotEqual(t, accountID1, accountID3)
}
