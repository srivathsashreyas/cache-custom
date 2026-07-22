// Package metrics collects process and per-tenant operational stats for INFO and Prometheus.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Collector tracks command rates/latency and exposes Prometheus text + snapshot stats.
type Collector struct {
	start time.Time

	// global
	commandsTotal atomic.Uint64
	errorsTotal   atomic.Uint64
	// latency in nanoseconds (sum + count for mean)
	latencySumNs atomic.Uint64
	latencyCount atomic.Uint64

	// total connections accepted (monotonic)
	connsAccepted atomic.Uint64
	connsClosed   atomic.Uint64

	mu      sync.Mutex
	tenants map[string]*tenantCounters
}

type tenantCounters struct {
	commands   atomic.Uint64
	errors     atomic.Uint64
	latencySum atomic.Uint64
	latencyN   atomic.Uint64
}

// New returns a collector starting at now.
func New() *Collector {
	return &Collector{
		start:   time.Now(),
		tenants: make(map[string]*tenantCounters),
	}
}

// StartTime is process/collector start.
func (c *Collector) StartTime() time.Time {
	if c == nil {
		return time.Time{}
	}
	return c.start
}

// UptimeSeconds since collector start.
func (c *Collector) UptimeSeconds() int64 {
	if c == nil {
		return 0
	}
	return int64(time.Since(c.start).Seconds())
}

// ObserveCommand records one command execution.
// tenant may be empty for unauthenticated connections.
func (c *Collector) ObserveCommand(tenant, command string, d time.Duration, isError bool) {
	if c == nil {
		return
	}
	c.commandsTotal.Add(1)
	ns := uint64(d.Nanoseconds())
	if ns > 0 {
		c.latencySumNs.Add(ns)
		c.latencyCount.Add(1)
	}
	if isError {
		c.errorsTotal.Add(1)
	}
	key := tenant
	if key == "" {
		key = "_none"
	}
	tc := c.tenant(key)
	tc.commands.Add(1)
	if ns > 0 {
		tc.latencySum.Add(ns)
		tc.latencyN.Add(1)
	}
	if isError {
		tc.errors.Add(1)
	}
	_ = command // reserved for per-command series later without cardinality explosion
}

// ConnAccepted increments accepted connection counter.
func (c *Collector) ConnAccepted() {
	if c != nil {
		c.connsAccepted.Add(1)
	}
}

// ConnClosed increments closed connection counter.
func (c *Collector) ConnClosed() {
	if c != nil {
		c.connsClosed.Add(1)
	}
}

func (c *Collector) tenant(name string) *tenantCounters {
	c.mu.Lock()
	defer c.mu.Unlock()
	tc, ok := c.tenants[name]
	if !ok {
		tc = &tenantCounters{}
		c.tenants[name] = tc
	}
	return tc
}

// Snapshot is a point-in-time view for INFO.
type Snapshot struct {
	UptimeSec     int64
	CommandsTotal uint64
	ErrorsTotal   uint64
	// MeanLatencyMicros is 0 if no samples.
	MeanLatencyMicros uint64
	ConnsAccepted     uint64
	ConnsClosed       uint64
	// PerTenant command totals (tenant name -> commands).
	TenantCommands map[string]uint64
	TenantErrors   map[string]uint64
}

// Snapshot returns counters for INFO.
func (c *Collector) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	s := Snapshot{
		UptimeSec:         c.UptimeSeconds(),
		CommandsTotal:     c.commandsTotal.Load(),
		ErrorsTotal:       c.errorsTotal.Load(),
		ConnsAccepted:     c.connsAccepted.Load(),
		ConnsClosed:       c.connsClosed.Load(),
		TenantCommands:    make(map[string]uint64),
		TenantErrors:      make(map[string]uint64),
	}
	n := c.latencyCount.Load()
	if n > 0 {
		s.MeanLatencyMicros = c.latencySumNs.Load() / n / 1000
	}
	c.mu.Lock()
	for name, tc := range c.tenants {
		s.TenantCommands[name] = tc.commands.Load()
		s.TenantErrors[name] = tc.errors.Load()
	}
	c.mu.Unlock()
	return s
}

// TenantStat is live store-facing data for Prometheus gauges.
type TenantStat struct {
	Name       string
	UsedMemory uint64
	MaxMemory  uint64
	Keys       uint64
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	ConnCount  int
}

// WritePrometheus writes Prometheus text exposition to w.
// tenants is optional live gauge data; connected is current open TCP clients.
func (c *Collector) WritePrometheus(w io.Writer, connected int, maxClients int, tenants []TenantStat) error {
	if c == nil {
		c = &Collector{start: time.Now(), tenants: map[string]*tenantCounters{}}
	}
	snap := c.Snapshot()
	var b strings.Builder

	writeHelp := func(name, typ, help string) {
		b.WriteString("# HELP ")
		b.WriteString(name)
		b.WriteString(" ")
		b.WriteString(help)
		b.WriteString("\n# TYPE ")
		b.WriteString(name)
		b.WriteString(" ")
		b.WriteString(typ)
		b.WriteString("\n")
	}

	writeHelp("cache_uptime_seconds", "gauge", "Seconds since process metrics started")
	fmt.Fprintf(&b, "cache_uptime_seconds %d\n", snap.UptimeSec)

	writeHelp("cache_commands_total", "counter", "Total RESP commands processed")
	fmt.Fprintf(&b, "cache_commands_total %d\n", snap.CommandsTotal)

	writeHelp("cache_command_errors_total", "counter", "Commands that returned a RESP error")
	fmt.Fprintf(&b, "cache_command_errors_total %d\n", snap.ErrorsTotal)

	writeHelp("cache_command_duration_mean_microseconds", "gauge", "Mean command latency in microseconds")
	fmt.Fprintf(&b, "cache_command_duration_mean_microseconds %d\n", snap.MeanLatencyMicros)

	writeHelp("cache_connections", "gauge", "Currently open client connections")
	fmt.Fprintf(&b, "cache_connections %d\n", connected)

	writeHelp("cache_connections_max", "gauge", "Configured global max clients (0=unlimited)")
	fmt.Fprintf(&b, "cache_connections_max %d\n", maxClients)

	writeHelp("cache_connections_accepted_total", "counter", "TCP connections accepted")
	fmt.Fprintf(&b, "cache_connections_accepted_total %d\n", snap.ConnsAccepted)

	writeHelp("cache_connections_closed_total", "counter", "TCP connections closed")
	fmt.Fprintf(&b, "cache_connections_closed_total %d\n", snap.ConnsClosed)

	// Per-tenant command counters from collector
	writeHelp("cache_tenant_commands_total", "counter", "Commands executed while bound to tenant")
	names := make([]string, 0, len(snap.TenantCommands))
	for n := range snap.TenantCommands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		label := promLabel(n)
		fmt.Fprintf(&b, "cache_tenant_commands_total{tenant=%q} %d\n", label, snap.TenantCommands[n])
	}

	writeHelp("cache_tenant_command_errors_total", "counter", "Error replies while bound to tenant")
	for _, n := range names {
		label := promLabel(n)
		fmt.Fprintf(&b, "cache_tenant_command_errors_total{tenant=%q} %d\n", label, snap.TenantErrors[n])
	}

	// Live store gauges
	writeHelp("cache_memory_used_bytes", "gauge", "Tenant used memory bytes")
	writeHelp("cache_memory_max_bytes", "gauge", "Tenant maxmemory bytes")
	writeHelp("cache_keys", "gauge", "Approximate key count")
	writeHelp("cache_hits_total", "counter", "Keyspace hits")
	writeHelp("cache_misses_total", "counter", "Keyspace misses")
	writeHelp("cache_evictions_total", "counter", "Keys evicted under maxmemory")
	writeHelp("cache_tenant_connections", "gauge", "AUTH-bound connections for tenant")
	writeHelp("cache_hit_ratio", "gauge", "hits/(hits+misses); 0 if no samples")

	sort.Slice(tenants, func(i, j int) bool { return tenants[i].Name < tenants[j].Name })
	for _, t := range tenants {
		label := promLabel(t.Name)
		fmt.Fprintf(&b, "cache_memory_used_bytes{tenant=%q} %d\n", label, t.UsedMemory)
		fmt.Fprintf(&b, "cache_memory_max_bytes{tenant=%q} %d\n", label, t.MaxMemory)
		fmt.Fprintf(&b, "cache_keys{tenant=%q} %d\n", label, t.Keys)
		fmt.Fprintf(&b, "cache_hits_total{tenant=%q} %d\n", label, t.Hits)
		fmt.Fprintf(&b, "cache_misses_total{tenant=%q} %d\n", label, t.Misses)
		fmt.Fprintf(&b, "cache_evictions_total{tenant=%q} %d\n", label, t.Evictions)
		fmt.Fprintf(&b, "cache_tenant_connections{tenant=%q} %d\n", label, t.ConnCount)
		var ratio float64
		den := t.Hits + t.Misses
		if den > 0 {
			ratio = float64(t.Hits) / float64(den)
		}
		fmt.Fprintf(&b, "cache_hit_ratio{tenant=%q} %.6f\n", label, ratio)
	}

	_, err := io.WriteString(w, b.String())
	return err
}

func promLabel(s string) string {
	// Prometheus label values are quoted; escape backslash and quote.
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}
