// Package tenant manages multi-tenant config, auth, and per-tenant stores.
package tenant

import (
	"fmt"
	"sync"
	"time"

	"cache-custom/internal/pubsub"
	"cache-custom/internal/store"
)

// Status is tenant lifecycle state.
type Status int

const (
	StatusActive Status = iota
	StatusDisabled
)

// Config is boot-time tenant definition (from config file).
type Config struct {
	// Name is the AUTH username and display name.
	Name string
	// Password is the AUTH secret for this tenant.
	Password string
	AppID    uint64
	MaxMemory uint64
	MaxTTL    time.Duration // 0 = no ceiling
	ShardCount int
	Strategy   store.Strategy
	Policy     store.EvictionPolicy
	// Disabled tenants reject AUTH and data commands if already bound.
	Disabled bool
}

// Tenant is one isolated keyspace + limits.
type Tenant struct {
	Name     string
	AppID    uint64
	Password string
	Status   Status
	DB       *store.DB
	PubSub   *pubsub.Hub

	// Cached config for INFO/stats (limits do not change at runtime in M3).
	MaxMemory uint64
	Strategy  store.Strategy
	Policy    store.EvictionPolicy
	Shards    int
}

// Registry holds all tenants loaded from config.
type Registry struct {
	mu      sync.RWMutex
	byName  map[string]*Tenant // AUTH username -> tenant (case-sensitive)
	order   []*Tenant
}

// NewRegistry builds tenants and their stores. Caller should Close().
func NewRegistry(cfgs []Config) (*Registry, error) {
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("tenant: at least one tenant required")
	}
	r := &Registry{byName: make(map[string]*Tenant, len(cfgs))}
	seen := make(map[string]struct{}, len(cfgs))
	for _, c := range cfgs {
		if c.Name == "" {
			return nil, fmt.Errorf("tenant: Name is required")
		}
		if _, dup := seen[c.Name]; dup {
			return nil, fmt.Errorf("tenant: duplicate Name %q", c.Name)
		}
		seen[c.Name] = struct{}{}
		if c.Password == "" {
			return nil, fmt.Errorf("tenant %q: Password is required", c.Name)
		}
		sc := c.ShardCount
		if sc < 1 {
			sc = 4
		}
		st := c.Strategy
		if st < store.StrategyGlobalTrack || st > store.StrategyShardBudget {
			st = store.StrategyGlobalTrack
		}
		pol := c.Policy
		if pol == "" {
			pol = store.PolicyAllKeysLRU
		}
		db := store.New(store.Config{
			MaxMemory:      c.MaxMemory,
			ShardCount:     sc,
			Strategy:       st,
			Policy:         pol,
			MaxTTL:         c.MaxTTL,
			ExpiryInterval: time.Second,
		})
		status := StatusActive
		if c.Disabled {
			status = StatusDisabled
		}
		t := &Tenant{
			Name:      c.Name,
			AppID:     c.AppID,
			Password:  c.Password,
			Status:    status,
			DB:        db,
			PubSub:    pubsub.NewHub(),
			MaxMemory: c.MaxMemory,
			Strategy:  st,
			Policy:    pol,
			Shards:    sc,
		}
		r.byName[c.Name] = t
		r.order = append(r.order, t)
	}
	return r, nil
}

// Close shuts down all tenant stores.
func (r *Registry) Close() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.order {
		t.DB.Close()
	}
}

// Authenticate validates AUTH username/password and returns the tenant.
func (r *Registry) Authenticate(username, password string) (*Tenant, error) {
	r.mu.RLock()
	t, ok := r.byName[username]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("WRONGPASS invalid username-password pair")
	}
	if t.Status == StatusDisabled {
		return nil, fmt.Errorf("ERR tenant is disabled")
	}
	if t.Password != password {
		return nil, fmt.Errorf("WRONGPASS invalid username-password pair")
	}
	return t, nil
}

// Get returns a tenant by name (not for auth).
func (r *Registry) Get(name string) (*Tenant, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byName[name]
	return t, ok
}

// All returns tenants in config order.
func (r *Registry) All() []*Tenant {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Tenant, len(r.order))
	copy(out, r.order)
	return out
}

// Len returns tenant count.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.order)
}
