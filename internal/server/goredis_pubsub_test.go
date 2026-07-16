package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"cache-custom/internal/command"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

// startPubSubServer boots a full AUTH + Pub/Sub capable server for go-redis tests.
func startPubSubServer(t *testing.T) string {
	t.Helper()
	tenants, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, Policy: store.PolicyAllKeysLRU, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, Policy: store.PolicyAllKeysLRU, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tenants.Close)

	reg := command.NewRegistry()
	command.RegisterDefaults(reg, tenants)
	command.RegisterAuth(reg, tenants)
	command.RegisterStringCommands(reg)
	command.RegisterPubSub(reg)

	s := New("127.0.0.1:0", reg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not accept")
	return addr
}

func goRedisOpts(addr, user, pass string) *redis.Options {
	return &redis.Options{
		Addr:            addr,
		Username:        user,
		Password:        pass,
		DialTimeout:     time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    2 * time.Second,
		DisableIdentity: true,
		Protocol:        2,
	}
}

// TestGoRedisPubSubSubscribePublish is an integration test using github.com/redis/go-redis.
func TestGoRedisPubSubSubscribePublish(t *testing.T) {
	addr := startPubSubServer(t)
	ctx := context.Background()

	subClient := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = subClient.Close() })
	pubClient := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = pubClient.Close() })

	// go-redis AUTH is applied on connection via Username/Password (AUTH user pass).
	if err := subClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("sub ping: %v", err)
	}
	if err := pubClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("pub ping: %v", err)
	}

	pubsub := subClient.Subscribe(ctx, "news")
	t.Cleanup(func() { _ = pubsub.Close() })

	// Wait until subscription is active (go-redis receives subscribe confirm).
	if _, err := pubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("receive subscribe confirm: %v", err)
	}

	n, err := pubClient.Publish(ctx, "news", "hello-go-redis").Result()
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 1 {
		t.Fatalf("receivers=%d want 1", n)
	}

	msg, err := pubsub.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatalf("receive message: %v", err)
	}
	m, ok := msg.(*redis.Message)
	if !ok {
		t.Fatalf("got %T %+v", msg, msg)
	}
	if m.Channel != "news" || m.Payload != "hello-go-redis" {
		t.Fatalf("%+v", m)
	}
}

func TestGoRedisPubSubPatternSubscribe(t *testing.T) {
	addr := startPubSubServer(t)
	ctx := context.Background()

	subClient := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = subClient.Close() })
	pubClient := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = pubClient.Close() })

	pubsub := subClient.PSubscribe(ctx, "news.*")
	t.Cleanup(func() { _ = pubsub.Close() })
	if _, err := pubsub.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("psubscribe confirm: %v", err)
	}

	n, err := pubClient.Publish(ctx, "news.42", "pat").Result()
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 1 {
		t.Fatalf("receivers=%d", n)
	}

	msg, err := pubsub.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	m, ok := msg.(*redis.Message)
	if !ok {
		t.Fatalf("%T", msg)
	}
	if m.Pattern != "news.*" || m.Channel != "news.42" || m.Payload != "pat" {
		t.Fatalf("%+v", m)
	}
}

func TestGoRedisPubSubTenantIsolation(t *testing.T) {
	addr := startPubSubServer(t)
	ctx := context.Background()

	subA := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = subA.Close() })
	pubB := redis.NewClient(goRedisOpts(addr, "App2", "p2"))
	t.Cleanup(func() { _ = pubB.Close() })

	ps := subA.Subscribe(ctx, "shared")
	t.Cleanup(func() { _ = ps.Close() })
	if _, err := ps.ReceiveTimeout(ctx, time.Second); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	n, err := pubB.Publish(ctx, "shared", "nope").Result()
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 0 {
		t.Fatalf("cross-tenant receivers=%d want 0", n)
	}

	// No message should arrive for A.
	_, err = ps.ReceiveTimeout(ctx, 150*time.Millisecond)
	if err == nil {
		t.Fatal("tenant A must not receive tenant B publish")
	}
}

func TestGoRedisPubSubZeroSubscribers(t *testing.T) {
	addr := startPubSubServer(t)
	ctx := context.Background()
	pub := redis.NewClient(goRedisOpts(addr, "App1", "p1"))
	t.Cleanup(func() { _ = pub.Close() })

	n, err := pub.Publish(ctx, "empty", "x").Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("got %d", n)
	}
}
