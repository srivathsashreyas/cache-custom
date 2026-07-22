package command

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"cache-custom/internal/metrics"
	"cache-custom/internal/protocol"
	"cache-custom/internal/tenant"
)

// RuntimeInfo supplies live data for INFO sections (M8).
// All fields optional; missing pieces omit or zero-fill the related lines.
type RuntimeInfo struct {
	Tenants *tenant.Registry
	// Persist
	PersistMode   string
	PersistDir    string
	AOFFsync      string
	LastSaveUnix  func() int64
	// Clients
	ConnectedClients func() int
	MaxClients       int
	// Process
	Addr        string
	StartTime   time.Time
	TLSEnabled  bool
	Profile     string
	RequireAuth bool
	// Metrics for command rate / latency
	Metrics *metrics.Collector
}

// makeInfoHandler builds Redis-style INFO with sections.
func makeInfoHandler(rt *RuntimeInfo) Handler {
	return func(ctx *Context, args []string) protocol.Value {
		if len(args) > 2 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'info' command")
		}
		section := "default"
		if len(args) == 2 {
			section = strings.ToLower(args[1])
		}
		if rt == nil {
			rt = &RuntimeInfo{}
		}

		var b strings.Builder
		writeServer := func() {
			b.WriteString("# Server\r\n")
			b.WriteString("redis_mode:standalone\r\n")
			b.WriteString("cache_custom_version:0.1.0\r\n")
			b.WriteString(fmt.Sprintf("go_version:%s\r\n", runtime.Version()))
			b.WriteString("executable:cache-custom\r\n")
			if rt.Addr != "" {
				b.WriteString(fmt.Sprintf("tcp_addr:%s\r\n", rt.Addr))
			}
			if !rt.StartTime.IsZero() {
				b.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", int64(time.Since(rt.StartTime).Seconds())))
			} else if rt.Metrics != nil {
				b.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", rt.Metrics.UptimeSeconds()))
			}
			b.WriteString(fmt.Sprintf("tls_enabled:%v\r\n", rt.TLSEnabled))
			if rt.Profile != "" {
				b.WriteString(fmt.Sprintf("security_profile:%s\r\n", rt.Profile))
			}
			b.WriteString(fmt.Sprintf("require_auth:%v\r\n", rt.RequireAuth))
			b.WriteString("\r\n")
		}
		writeClients := func() {
			b.WriteString("# Clients\r\n")
			connected := 0
			if rt.ConnectedClients != nil {
				connected = rt.ConnectedClients()
			}
			b.WriteString(fmt.Sprintf("connected_clients:%d\r\n", connected))
			b.WriteString(fmt.Sprintf("maxclients:%d\r\n", rt.MaxClients))
			if rt.Metrics != nil {
				snap := rt.Metrics.Snapshot()
				b.WriteString(fmt.Sprintf("total_connections_received:%d\r\n", snap.ConnsAccepted))
			}
			b.WriteString("\r\n")
		}
		writeMemory := func() {
			b.WriteString("# Memory\r\n")
			var used, max uint64
			if rt.Tenants != nil {
				for _, t := range rt.Tenants.All() {
					u, m, _, _, _, _ := t.DB.Stats()
					used += u
					max += m
				}
			}
			b.WriteString(fmt.Sprintf("used_memory:%d\r\n", used))
			b.WriteString(fmt.Sprintf("maxmemory:%d\r\n", max))
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			b.WriteString(fmt.Sprintf("process_resident_memory_bytes:%d\r\n", ms.Sys))
			b.WriteString(fmt.Sprintf("process_heap_alloc_bytes:%d\r\n", ms.HeapAlloc))
			b.WriteString("\r\n")
		}
		writeStats := func() {
			b.WriteString("# Stats\r\n")
			var hits, misses, evictions uint64
			if rt.Tenants != nil {
				for _, t := range rt.Tenants.All() {
					_, _, _, h, m, e := t.DB.Stats()
					hits += h
					misses += m
					evictions += e
				}
			}
			b.WriteString(fmt.Sprintf("keyspace_hits:%d\r\n", hits))
			b.WriteString(fmt.Sprintf("keyspace_misses:%d\r\n", misses))
			b.WriteString(fmt.Sprintf("evicted_keys:%d\r\n", evictions))
			den := hits + misses
			ratio := 0.0
			if den > 0 {
				ratio = float64(hits) / float64(den)
			}
			b.WriteString(fmt.Sprintf("hit_rate:%.4f\r\n", ratio))
			if rt.Metrics != nil {
				snap := rt.Metrics.Snapshot()
				b.WriteString(fmt.Sprintf("total_commands_processed:%d\r\n", snap.CommandsTotal))
				b.WriteString(fmt.Sprintf("total_error_replies:%d\r\n", snap.ErrorsTotal))
				b.WriteString(fmt.Sprintf("instantaneous_ops_per_sec:%d\r\n", opsPerSec(snap)))
				b.WriteString(fmt.Sprintf("mean_command_usec:%d\r\n", snap.MeanLatencyMicros))
			}
			b.WriteString("\r\n")
		}
		writePersistence := func() {
			b.WriteString("# Persistence\r\n")
			mode := rt.PersistMode
			if mode == "" {
				mode = "none"
			}
			b.WriteString(fmt.Sprintf("persistence_mode:%s\r\n", mode))
			if rt.PersistDir != "" {
				b.WriteString(fmt.Sprintf("persistence_dir:%s\r\n", rt.PersistDir))
			}
			if rt.AOFFsync != "" {
				b.WriteString(fmt.Sprintf("aof_fsync:%s\r\n", rt.AOFFsync))
			}
			var last int64
			if rt.LastSaveUnix != nil {
				last = rt.LastSaveUnix()
			}
			b.WriteString(fmt.Sprintf("rdb_last_save_time:%d\r\n", last))
			b.WriteString("\r\n")
		}
		writeTenants := func() {
			b.WriteString("# Tenants\r\n")
			if rt.Tenants == nil {
				b.WriteString("tenant_count:0\r\n\r\n")
				return
			}
			b.WriteString(fmt.Sprintf("tenant_count:%d\r\n", rt.Tenants.Len()))
			var snap metrics.Snapshot
			if rt.Metrics != nil {
				snap = rt.Metrics.Snapshot()
			}
			for _, t := range rt.Tenants.All() {
				used, max, keys, hits, misses, evictions := t.DB.Stats()
				status := "active"
				if t.Status != tenant.StatusActive {
					status = "disabled"
				}
				prefix := "tenant_" + t.Name + "_"
				b.WriteString(fmt.Sprintf("%sstatus:%s\r\n", prefix, status))
				b.WriteString(fmt.Sprintf("%sused_memory:%d\r\n", prefix, used))
				b.WriteString(fmt.Sprintf("%smax_memory:%d\r\n", prefix, max))
				b.WriteString(fmt.Sprintf("%skeys:%d\r\n", prefix, keys))
				b.WriteString(fmt.Sprintf("%shits:%d\r\n", prefix, hits))
				b.WriteString(fmt.Sprintf("%smisses:%d\r\n", prefix, misses))
				b.WriteString(fmt.Sprintf("%sevictions:%d\r\n", prefix, evictions))
				den := hits + misses
				hr := 0.0
				if den > 0 {
					hr = float64(hits) / float64(den)
				}
				b.WriteString(fmt.Sprintf("%shit_rate:%.4f\r\n", prefix, hr))
				b.WriteString(fmt.Sprintf("%sstrategy:%d\r\n", prefix, int(t.Strategy)))
				b.WriteString(fmt.Sprintf("%seviction_policy:%s\r\n", prefix, t.Policy))
				b.WriteString(fmt.Sprintf("%sshards:%d\r\n", prefix, t.Shards))
				b.WriteString(fmt.Sprintf("%sconnected_clients:%d\r\n", prefix, t.ConnCount()))
				b.WriteString(fmt.Sprintf("%smax_clients:%d\r\n", prefix, t.MaxClients))
				if snap.TenantCommands != nil {
					b.WriteString(fmt.Sprintf("%scommands_processed:%d\r\n", prefix, snap.TenantCommands[t.Name]))
					b.WriteString(fmt.Sprintf("%serror_replies:%d\r\n", prefix, snap.TenantErrors[t.Name]))
				}
			}
			if ctx != nil && ctx.Tenant != nil {
				b.WriteString(fmt.Sprintf("current_tenant:%s\r\n", ctx.Tenant.Name))
			}
			b.WriteString("\r\n")
		}

		switch section {
		case "server":
			writeServer()
		case "clients":
			writeClients()
		case "memory":
			writeMemory()
		case "stats":
			writeStats()
		case "persistence":
			writePersistence()
		case "tenants":
			writeTenants()
		case "default", "all":
			writeServer()
			writeClients()
			writeMemory()
			writeStats()
			writePersistence()
			writeTenants()
		default:
			return protocol.BulkStringValue("")
		}
		return protocol.BulkStringValue(b.String())
	}
}

func opsPerSec(snap metrics.Snapshot) uint64 {
	if snap.UptimeSec <= 0 {
		return 0
	}
	return snap.CommandsTotal / uint64(snap.UptimeSec)
}

// RegisterTenantStats registers TENANTSTATS (alias summary for the bound tenant or all if admin-less: all when AUTH'd shows own; without AUTH shows all for ops).
// Unauthenticated: all tenants (same as INFO tenants subset). Authenticated: only current tenant.
func RegisterTenantStats(r *Registry, tenants *tenant.Registry, coll *metrics.Collector) {
	r.Register("TENANTSTATS", func(ctx *Context, args []string) protocol.Value {
		if len(args) != 1 {
			return protocol.ErrorValue("ERR wrong number of arguments for 'tenantstats' command")
		}
		if tenants == nil {
			return protocol.ArrayValue()
		}
		var list []*tenant.Tenant
		if ctx != nil && ctx.Tenant != nil {
			list = []*tenant.Tenant{ctx.Tenant}
		} else {
			list = tenants.All()
		}
		var snap metrics.Snapshot
		if coll != nil {
			snap = coll.Snapshot()
		}
		elems := make([]protocol.Value, 0, len(list))
		for _, t := range list {
			used, max, keys, hits, misses, evictions := t.DB.Stats()
			var cmds, errs uint64
			if snap.TenantCommands != nil {
				cmds = snap.TenantCommands[t.Name]
				errs = snap.TenantErrors[t.Name]
			}
			// flat key-value array like Redis MODULE INFO style
			elems = append(elems, protocol.ArrayValue(
				protocol.BulkStringValue("name"), protocol.BulkStringValue(t.Name),
				protocol.BulkStringValue("used_memory"), protocol.BulkStringValue(fmt.Sprintf("%d", used)),
				protocol.BulkStringValue("max_memory"), protocol.BulkStringValue(fmt.Sprintf("%d", max)),
				protocol.BulkStringValue("keys"), protocol.BulkStringValue(fmt.Sprintf("%d", keys)),
				protocol.BulkStringValue("hits"), protocol.BulkStringValue(fmt.Sprintf("%d", hits)),
				protocol.BulkStringValue("misses"), protocol.BulkStringValue(fmt.Sprintf("%d", misses)),
				protocol.BulkStringValue("evictions"), protocol.BulkStringValue(fmt.Sprintf("%d", evictions)),
				protocol.BulkStringValue("connected_clients"), protocol.BulkStringValue(fmt.Sprintf("%d", t.ConnCount())),
				protocol.BulkStringValue("commands_processed"), protocol.BulkStringValue(fmt.Sprintf("%d", cmds)),
				protocol.BulkStringValue("error_replies"), protocol.BulkStringValue(fmt.Sprintf("%d", errs)),
			))
		}
		return protocol.ArrayValue(elems...)
	})
}
