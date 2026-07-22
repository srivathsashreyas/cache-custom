package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestObserveAndPrometheus(t *testing.T) {
	c := New()
	c.ObserveCommand("App1", "GET", 100*time.Microsecond, false)
	c.ObserveCommand("App1", "GET", 200*time.Microsecond, true)
	c.ObserveCommand("", "PING", time.Millisecond, false)
	c.ConnAccepted()
	c.ConnClosed()

	snap := c.Snapshot()
	if snap.CommandsTotal != 3 || snap.ErrorsTotal != 1 {
		t.Fatalf("snap %+v", snap)
	}
	if snap.TenantCommands["App1"] != 2 {
		t.Fatalf("tenant cmds %+v", snap.TenantCommands)
	}

	var buf bytes.Buffer
	err := c.WritePrometheus(&buf, 2, 100, []TenantStat{{
		Name: "App1", UsedMemory: 10, MaxMemory: 100, Keys: 1, Hits: 8, Misses: 2, Evictions: 0, ConnCount: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"cache_commands_total 3",
		"cache_command_errors_total 1",
		"cache_connections 2",
		`cache_tenant_commands_total{tenant="App1"} 2`,
		`cache_memory_used_bytes{tenant="App1"} 10`,
		`cache_hit_ratio{tenant="App1"} 0.800000`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}
