package command

import (
	"sync"
	"testing"
	"time"

	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

type captureWriter struct {
	mu   sync.Mutex
	vals []protocol.Value
}

func (c *captureWriter) WriteValue(v protocol.Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vals = append(c.vals, v)
	return nil
}

func (c *captureWriter) last() protocol.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.vals) == 0 {
		return protocol.Value{}
	}
	return c.vals[len(c.vals)-1]
}

func pubsubReg(t *testing.T) (*Registry, *tenant.Registry) {
	t.Helper()
	tenants, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, Policy: store.PolicyAllKeysLRU, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, Policy: store.PolicyAllKeysLRU, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tenants.Close)
	r := NewRegistry()
	RegisterDefaults(r, tenants)
	RegisterAuth(r, tenants)
	RegisterStringCommands(r)
	RegisterPubSub(r)
	return r, tenants
}

func authWriter(t *testing.T, r *Registry, user, pass string) (*Context, *captureWriter) {
	t.Helper()
	w := &captureWriter{}
	ctx := &Context{Writer: w}
	v := r.Dispatch(ctx, []string{"AUTH", user, pass})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("auth %+v", v)
	}
	return ctx, w
}

func TestPubSubPublishSubscribe(t *testing.T) {
	r, _ := pubsubReg(t)
	sub, sw := authWriter(t, r, "App1", "p1")
	pub, _ := authWriter(t, r, "App1", "p1")

	sub.Multi = nil
	r.Dispatch(sub, []string{"SUBSCRIBE", "news"})
	if len(sub.Multi) != 1 || sub.Multi[0].Array[0].Str != "subscribe" {
		t.Fatalf("%+v", sub.Multi)
	}

	v := r.Dispatch(pub, []string{"PUBLISH", "news", "hello"})
	if v.Type != protocol.Integer || v.Int != 1 {
		t.Fatalf("publish %+v", v)
	}
	// Wait for deliver
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sw.mu.Lock()
		n := len(sw.vals)
		sw.mu.Unlock()
		// AUTH reply not stored in writer for Dispatch path - only Deliver uses writer.
		// Subscribe confirms go via Multi written by server; Deliver only for publish.
		// In unit test we don't run server Multi write — publish uses Deliver.
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.vals) < 1 {
		t.Fatal("expected pushed message")
	}
	msg := sw.vals[len(sw.vals)-1]
	if msg.Array[0].Str != "message" || msg.Array[2].Str != "hello" {
		t.Fatalf("%+v", msg)
	}
}

func TestPubSubTenantIsolation(t *testing.T) {
	r, _ := pubsubReg(t)
	a, aw := authWriter(t, r, "App1", "p1")
	b, _ := authWriter(t, r, "App2", "p2")
	r.Dispatch(a, []string{"SUBSCRIBE", "ch"})
	v := r.Dispatch(b, []string{"PUBLISH", "ch", "x"})
	if v.Int != 0 {
		t.Fatalf("cross-tenant publish should hit 0 receivers, got %d", v.Int)
	}
	time.Sleep(20 * time.Millisecond)
	if aw.len() != 0 {
		t.Fatal("tenant A should not receive B publishes on isolated hubs")
	}
}

func (c *captureWriter) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.vals)
}

func TestSubscribeModeBlocksGET(t *testing.T) {
	r, _ := pubsubReg(t)
	ctx, _ := authWriter(t, r, "App1", "p1")
	r.Dispatch(ctx, []string{"SUBSCRIBE", "c"})
	v := r.Dispatch(ctx, []string{"GET", "k"})
	if v.Type != protocol.Error {
		t.Fatalf("%+v", v)
	}
}

func TestPubSubNoAuth(t *testing.T) {
	r, _ := pubsubReg(t)
	v := r.Dispatch(&Context{Writer: &captureWriter{}}, []string{"PUBLISH", "c", "m"})
	if v.Type != protocol.Error || v.Str != "NOAUTH Authentication required." {
		t.Fatalf("%+v", v)
	}
}

func TestPSubscribe(t *testing.T) {
	r, _ := pubsubReg(t)
	sub, sw := authWriter(t, r, "App1", "p1")
	pub, _ := authWriter(t, r, "App1", "p1")
	r.Dispatch(sub, []string{"PSUBSCRIBE", "news.*"})
	v := r.Dispatch(pub, []string{"PUBLISH", "news.1", "hi"})
	if v.Int != 1 {
		t.Fatalf("%+v", v)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && sw.len() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if sw.len() < 1 || sw.last().Array[0].Str != "pmessage" {
		t.Fatalf("%+v", sw.vals)
	}
}
