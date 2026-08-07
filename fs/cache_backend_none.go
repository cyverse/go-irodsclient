package fs

import "time"

// NoCacheBackend implements CacheBackend as a no-op (caching disabled)
type NoCacheBackend struct {
}

// NewNoCacheBackend creates a no-cache backend
func NewNoCacheBackend() *NoCacheBackend {
	return &NoCacheBackend{}
}

// GetNamespace returns a no-op namespace
func (n *NoCacheBackend) GetNamespace(namespace string) CacheNamespace {
	return &NoCacheNamespace{}
}

// DeleteNamespace is a no-op
func (n *NoCacheBackend) DeleteNamespace(namespace string) error {
	return nil
}

// Clear is a no-op
func (n *NoCacheBackend) Clear() error {
	return nil
}

// Close is a no-op
func (n *NoCacheBackend) Close() error {
	return nil
}

// NoCacheNamespace implements CacheNamespace as a no-op
type NoCacheNamespace struct {
}

// Get returns nil, false
func (n *NoCacheNamespace) Get(key string) (interface{}, bool, error) {
	return nil, false, nil
}

// Set is a no-op
func (n *NoCacheNamespace) Set(key string, value interface{}, ttl time.Duration) error {
	return nil
}

// Delete is a no-op
func (n *NoCacheNamespace) Delete(key string) error {
	return nil
}

// Exists returns false
func (n *NoCacheNamespace) Exists(key string) (bool, error) {
	return false, nil
}

// DeletePrefix is a no-op
func (n *NoCacheNamespace) DeletePrefix(prefix string) error {
	return nil
}

// GetPrefix returns empty map
func (n *NoCacheNamespace) GetPrefix(prefix string) (map[string]interface{}, error) {
	return make(map[string]interface{}), nil
}

// Clear is a no-op
func (n *NoCacheNamespace) Clear() error {
	return nil
}
