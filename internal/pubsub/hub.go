// Package pubsub implements tenant-scoped Redis-style Pub/Sub (in-process).
package pubsub

import (
	"path"
	"sync"

	"cache-custom/internal/protocol"
)

// Deliverer pushes a RESP value to a connection (must be safe for concurrent use).
type Deliverer interface {
	Deliver(v protocol.Value) error
}

// Client is one connection's subscription state on a hub.
type Client struct {
	hub *Hub
	d   Deliverer

	mu       sync.Mutex
	channels map[string]struct{}
	patterns map[string]struct{}
	closed   bool
}

// Hub is a per-tenant, concurrent-safe Pub/Sub registry.
type Hub struct {
	mu sync.RWMutex
	// channel name -> subscribers
	byChannel map[string]map[*Client]struct{}
	// pattern string -> subscribers
	byPattern map[string]map[*Client]struct{}
}

// NewHub creates an empty hub.
func NewHub() *Hub {
	return &Hub{
		byChannel: make(map[string]map[*Client]struct{}),
		byPattern: make(map[string]map[*Client]struct{}),
	}
}

// NewClient binds a connection deliverer to this hub.
func (h *Hub) NewClient(d Deliverer) *Client {
	return &Client{
		hub:      h,
		d:        d,
		channels: make(map[string]struct{}),
		patterns: make(map[string]struct{}),
	}
}

// SubCount returns total channel + pattern subscriptions for this client.
func (c *Client) SubCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.channels) + len(c.patterns)
}

// InSubscribeMode reports whether the client has any active subscription.
func (c *Client) InSubscribeMode() bool {
	return c.SubCount() > 0
}

// Close removes all subscriptions (connection teardown).
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	chans := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		chans = append(chans, ch)
	}
	pats := make([]string, 0, len(c.patterns))
	for p := range c.patterns {
		pats = append(pats, p)
	}
	c.channels = make(map[string]struct{})
	c.patterns = make(map[string]struct{})
	c.mu.Unlock()

	c.hub.unsubscribeChannels(c, chans...)
	c.hub.punsubscribePatterns(c, pats...)
}

// Subscribe adds channels; returns one confirmation payload per channel (name, total count).
func (c *Client) Subscribe(channels ...string) []SubConfirm {
	out := make([]SubConfirm, 0, len(channels))
	for _, ch := range channels {
		if ch == "" {
			continue
		}
		c.hub.subscribeChannel(c, ch)
		c.mu.Lock()
		c.channels[ch] = struct{}{}
		n := len(c.channels) + len(c.patterns)
		c.mu.Unlock()
		out = append(out, SubConfirm{Kind: "subscribe", Name: ch, Count: n})
	}
	return out
}

// Unsubscribe removes channels; empty means all. One confirm per removed (or all if empty).
func (c *Client) Unsubscribe(channels ...string) []SubConfirm {
	c.mu.Lock()
	if len(channels) == 0 {
		channels = make([]string, 0, len(c.channels))
		for ch := range c.channels {
			channels = append(channels, ch)
		}
	}
	c.mu.Unlock()

	out := make([]SubConfirm, 0, len(channels))
	for _, ch := range channels {
		c.hub.unsubscribeChannels(c, ch)
		c.mu.Lock()
		delete(c.channels, ch)
		n := len(c.channels) + len(c.patterns)
		c.mu.Unlock()
		out = append(out, SubConfirm{Kind: "unsubscribe", Name: ch, Count: n})
	}
	return out
}

// PSubscribe adds patterns.
func (c *Client) PSubscribe(patterns ...string) []SubConfirm {
	out := make([]SubConfirm, 0, len(patterns))
	for _, p := range patterns {
		if p == "" {
			continue
		}
		c.hub.psubscribePattern(c, p)
		c.mu.Lock()
		c.patterns[p] = struct{}{}
		n := len(c.channels) + len(c.patterns)
		c.mu.Unlock()
		out = append(out, SubConfirm{Kind: "psubscribe", Name: p, Count: n})
	}
	return out
}

// PUnsubscribe removes patterns; empty means all.
func (c *Client) PUnsubscribe(patterns ...string) []SubConfirm {
	c.mu.Lock()
	if len(patterns) == 0 {
		patterns = make([]string, 0, len(c.patterns))
		for p := range c.patterns {
			patterns = append(patterns, p)
		}
	}
	c.mu.Unlock()

	out := make([]SubConfirm, 0, len(patterns))
	for _, p := range patterns {
		c.hub.punsubscribePatterns(c, p)
		c.mu.Lock()
		delete(c.patterns, p)
		n := len(c.channels) + len(c.patterns)
		c.mu.Unlock()
		out = append(out, SubConfirm{Kind: "punsubscribe", Name: p, Count: n})
	}
	return out
}

// SubConfirm is a subscribe/unsubscribe acknowledgement.
type SubConfirm struct {
	Kind  string // subscribe, unsubscribe, psubscribe, punsubscribe
	Name  string // channel or pattern
	Count int
}

// ToValue formats a Redis-style confirmation array.
func (s SubConfirm) ToValue() protocol.Value {
	return protocol.ArrayValue(
		protocol.BulkStringValue(s.Kind),
		protocol.BulkStringValue(s.Name),
		protocol.IntegerValue(int64(s.Count)),
	)
}

func (h *Hub) subscribeChannel(c *Client, ch string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.byChannel[ch]
	if !ok {
		set = make(map[*Client]struct{})
		h.byChannel[ch] = set
	}
	set[c] = struct{}{}
}

func (h *Hub) unsubscribeChannels(c *Client, channels ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range channels {
		set, ok := h.byChannel[ch]
		if !ok {
			continue
		}
		delete(set, c)
		if len(set) == 0 {
			delete(h.byChannel, ch)
		}
	}
}

func (h *Hub) psubscribePattern(c *Client, pat string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.byPattern[pat]
	if !ok {
		set = make(map[*Client]struct{})
		h.byPattern[pat] = set
	}
	set[c] = struct{}{}
}

func (h *Hub) punsubscribePatterns(c *Client, patterns ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range patterns {
		set, ok := h.byPattern[p]
		if !ok {
			continue
		}
		delete(set, c)
		if len(set) == 0 {
			delete(h.byPattern, p)
		}
	}
}

// Publish delivers message to channel and matching pattern subscribers.
// Returns the number of clients that received the message (channel + pattern matches count separately per Redis).
func (h *Hub) Publish(channel, message string) int {
	h.mu.RLock()
	// Snapshot recipients to avoid holding lock during Deliver.
	var targets []struct {
		c       *Client
		pattern string // empty => plain message
	}
	if set, ok := h.byChannel[channel]; ok {
		for c := range set {
			targets = append(targets, struct {
				c       *Client
				pattern string
			}{c: c})
		}
	}
	for pat, set := range h.byPattern {
		if matchPattern(pat, channel) {
			for c := range set {
				targets = append(targets, struct {
					c       *Client
					pattern string
				}{c: c, pattern: pat})
			}
		}
	}
	h.mu.RUnlock()

	n := 0
	for _, t := range targets {
		var v protocol.Value
		if t.pattern == "" {
			v = protocol.ArrayValue(
				protocol.BulkStringValue("message"),
				protocol.BulkStringValue(channel),
				protocol.BulkStringValue(message),
			)
		} else {
			v = protocol.ArrayValue(
				protocol.BulkStringValue("pmessage"),
				protocol.BulkStringValue(t.pattern),
				protocol.BulkStringValue(channel),
				protocol.BulkStringValue(message),
			)
		}
		if err := t.c.deliver(v); err == nil {
			n++
		}
	}
	return n
}

func (c *Client) deliver(v protocol.Value) error {
	c.mu.Lock()
	closed := c.closed
	d := c.d
	c.mu.Unlock()
	if closed || d == nil {
		return errClosed
	}
	return d.Deliver(v)
}

var errClosed = errString("pubsub: client closed")

type errString string

func (e errString) Error() string { return string(e) }

// Channels returns active channel names (optionally filtered by glob pattern).
func (h *Hub) Channels(pattern string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.byChannel))
	for ch := range h.byChannel {
		if pattern == "" || matchPattern(pattern, ch) {
			out = append(out, ch)
		}
	}
	return out
}

// NumSub returns subscriber counts for each channel name.
func (h *Hub) NumSub(channels ...string) []int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]int64, len(channels))
	for i, ch := range channels {
		if set, ok := h.byChannel[ch]; ok {
			out[i] = int64(len(set))
		}
	}
	return out
}

// NumPat returns the number of unique pattern subscriptions (sum of pattern bindings).
func (h *Hub) NumPat() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var n int64
	for _, set := range h.byPattern {
		n += int64(len(set))
	}
	return n
}

// matchPattern uses path.Match-style globs (* and ?), suitable for Redis-like PSUBSCRIBE.
func matchPattern(pattern, channel string) bool {
	ok, err := path.Match(pattern, channel)
	return err == nil && ok
}
