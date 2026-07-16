package pubsub

import (
	"sync"
	"testing"

	"cache-custom/internal/protocol"
)

type memDeliverer struct {
	mu   sync.Mutex
	msgs []protocol.Value
}

func (m *memDeliverer) Deliver(v protocol.Value) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, v)
	return nil
}

func (m *memDeliverer) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.msgs)
}

func TestPublishSubscribe(t *testing.T) {
	h := NewHub()
	d := &memDeliverer{}
	c := h.NewClient(d)
	conf := c.Subscribe("news")
	if len(conf) != 1 || conf[0].Count != 1 {
		t.Fatalf("%+v", conf)
	}
	n := h.Publish("news", "hello")
	if n != 1 {
		t.Fatalf("receivers %d", n)
	}
	if d.len() != 1 {
		t.Fatalf("msgs %d", d.len())
	}
	if d.msgs[0].Array[0].Str != "message" || d.msgs[0].Array[2].Str != "hello" {
		t.Fatalf("%+v", d.msgs[0])
	}
}

func TestPublishZeroSubscribers(t *testing.T) {
	h := NewHub()
	if h.Publish("x", "y") != 0 {
		t.Fatal("expected 0")
	}
}

func TestPatternSubscribe(t *testing.T) {
	h := NewHub()
	d := &memDeliverer{}
	c := h.NewClient(d)
	c.PSubscribe("news.*")
	if h.Publish("news.1", "a") != 1 {
		t.Fatal("expected pattern match")
	}
	if h.Publish("other", "b") != 0 {
		t.Fatal("expected no match")
	}
	if d.len() != 1 || d.msgs[0].Array[0].Str != "pmessage" {
		t.Fatalf("%+v", d.msgs)
	}
}

func TestTenantIsolationSeparateHubs(t *testing.T) {
	h1, h2 := NewHub(), NewHub()
	d1, d2 := &memDeliverer{}, &memDeliverer{}
	c1, c2 := h1.NewClient(d1), h2.NewClient(d2)
	c1.Subscribe("shared")
	c2.Subscribe("shared")
	if h1.Publish("shared", "from1") != 1 {
		t.Fatal()
	}
	if d2.len() != 0 {
		t.Fatal("hub isolation broken")
	}
	if h2.Publish("shared", "from2") != 1 || d1.len() != 1 {
		t.Fatal()
	}
}

func TestUnsubscribeAllExits(t *testing.T) {
	h := NewHub()
	c := h.NewClient(&memDeliverer{})
	c.Subscribe("a", "b")
	if !c.InSubscribeMode() {
		t.Fatal()
	}
	c.Unsubscribe()
	if c.InSubscribeMode() {
		t.Fatal("expected exit sub mode")
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := h.NewClient(&memDeliverer{})
			c.Subscribe("ch")
			h.Publish("ch", "x")
			c.Unsubscribe("ch")
			c.Close()
		}()
	}
	wg.Wait()
}

func TestPubSubIntrospection(t *testing.T) {
	h := NewHub()
	c := h.NewClient(&memDeliverer{})
	c.Subscribe("a", "b")
	c.PSubscribe("x*")
	ch := h.Channels("")
	if len(ch) != 2 {
		t.Fatalf("%v", ch)
	}
	ns := h.NumSub("a", "z")
	if ns[0] != 1 || ns[1] != 0 {
		t.Fatalf("%v", ns)
	}
	if h.NumPat() != 1 {
		t.Fatalf("%d", h.NumPat())
	}
}
