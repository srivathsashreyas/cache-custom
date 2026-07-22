package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cache-custom/internal/metrics"
	"cache-custom/internal/tenant"
)

func TestHTTPOpsEndpoints(t *testing.T) {
	r, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1024, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	coll := metrics.New()
	coll.ObserveCommand("App1", "GET", time.Millisecond, false)

	var ready atomic.Bool
	ready.Store(true)
	ops := &HTTPOps{
		Metrics:    coll,
		Tenants:    r,
		Connected:  func() int { return 4 },
		MaxClients: 50,
		Ready:      &ready,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = ops.Serve(ln) }()
	t.Cleanup(func() {
		_ = ops.Shutdown(context.Background())
	})
	base := "http://" + ln.Addr().String()

	// healthz
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "ok") {
		t.Fatalf("healthz %d %s", resp.StatusCode, body)
	}

	// readyz ready
	resp, err = http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz want 200 got %d", resp.StatusCode)
	}
	ready.Store(false)
	resp, err = http.Get(base + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("readyz want 503 got %d", resp.StatusCode)
	}

	// metrics
	resp, err = http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	out := string(body)
	for _, want := range []string{
		"cache_commands_total",
		"cache_connections 4",
		`cache_memory_max_bytes{tenant="App1"}`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q\n%s", want, out)
		}
	}
}
