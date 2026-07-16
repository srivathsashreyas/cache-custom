package pubsub

import (
	"fmt"
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
	const workers = 32
	const payload = "burst"

	deliverers := make([]*memDeliverer, workers)
	clients := make([]*Client, workers)
	for i := 0; i < workers; i++ {
		deliverers[i] = &memDeliverer{}
		clients[i] = h.NewClient(deliverers[i])
	}

	// Concurrent subscribe — collect errors after Wait (t is not used inside goroutines).
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conf := clients[i].Subscribe("ch")
			if len(conf) != 1 {
				errCh <- fmt.Errorf("worker %d: subscribe confirms=%d", i, len(conf))
				return
			}
			if conf[0].Kind != "subscribe" || conf[0].Name != "ch" {
				errCh <- fmt.Errorf("worker %d: bad confirm %+v", i, conf[0])
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	ns := h.NumSub("ch")
	if len(ns) != 1 || ns[0] != int64(workers) {
		t.Fatalf("NumSub after subscribe: %v want %d", ns, workers)
	}

	// Single publish should reach every subscriber (Deliver is synchronous).
	got := h.Publish("ch", payload)
	if got != workers {
		t.Fatalf("Publish receivers=%d want %d", got, workers)
	}
	for i, d := range deliverers {
		if d.len() < 1 {
			t.Errorf("worker %d: expected at least one pushed message", i)
			continue
		}
		// Last message should be the burst publish (subscribe confirms are not Deliver'd).
		msg := d.msgs[d.len()-1]
		if len(msg.Array) < 3 || msg.Array[0].Str != "message" || msg.Array[2].Str != payload {
			t.Errorf("worker %d: bad message %+v", i, msg)
		}
	}

	// Concurrent unsubscribe + close; hub must end empty.
	errCh = make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conf := clients[i].Unsubscribe("ch")
			if len(conf) != 1 || conf[0].Kind != "unsubscribe" {
				errCh <- fmt.Errorf("worker %d: unsubscribe %+v", i, conf)
				return
			}
			clients[i].Close()
			if clients[i].InSubscribeMode() {
				errCh <- fmt.Errorf("worker %d: still in subscribe mode after close", i)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	if ns := h.NumSub("ch"); len(ns) != 1 || ns[0] != 0 {
		t.Fatalf("NumSub after cleanup: %v", ns)
	}
	if chs := h.Channels(""); len(chs) != 0 {
		t.Fatalf("Channels after cleanup: %v", chs)
	}
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
