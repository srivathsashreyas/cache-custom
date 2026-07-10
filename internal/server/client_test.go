package server

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"cache-custom/internal/command"
)

// startGoRedisServer boots a RESP server for language-client integration tests.
func startGoRedisServer(t *testing.T) string {
	t.Helper()
	reg := command.NewRegistry()
	command.RegisterDefaults(reg)
	s := New("127.0.0.1:0", reg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })

	// Wait until the accept loop is reachable.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not accept connections")
	return addr
}

func newGoRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		// Disable client-side features that assume full Redis (e.g. auto CLIENT SETINFO).
		DisableIdentity: true,
		Protocol:        2, // RESP2
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestGoRedisClientPing exercises acceptance: a real language client can PING.
func TestGoRedisClientPing(t *testing.T) {
	addr := startGoRedisServer(t)
	rdb := newGoRedisClient(t, addr)
	ctx := context.Background()

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if pong != "PONG" {
		t.Fatalf("got %q", pong)
	}
}

func TestGoRedisClientEcho(t *testing.T) {
	addr := startGoRedisServer(t)
	rdb := newGoRedisClient(t, addr)
	ctx := context.Background()

	out, err := rdb.Echo(ctx, "from-go-redis").Result()
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if out != "from-go-redis" {
		t.Fatalf("got %q", out)
	}
}

func TestGoRedisClientPipeline(t *testing.T) {
	addr := startGoRedisServer(t)
	rdb := newGoRedisClient(t, addr)
	ctx := context.Background()

	pipe := rdb.Pipeline()
	pingCmd := pipe.Ping(ctx)
	echoCmd := pipe.Echo(ctx, "pipe")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Pipeline Exec: %v", err)
	}
	if pingCmd.Val() != "PONG" {
		t.Fatalf("ping: %q", pingCmd.Val())
	}
	if echoCmd.Val() != "pipe" {
		t.Fatalf("echo: %q", echoCmd.Val())
	}
}

func TestGoRedisClientCommandAndInfo(t *testing.T) {
	addr := startGoRedisServer(t)
	rdb := newGoRedisClient(t, addr)
	ctx := context.Background()

	n, err := rdb.Do(ctx, "COMMAND", "COUNT").Int()
	if err != nil {
		t.Fatalf("COMMAND COUNT: %v", err)
	}
	if n != 5 {
		t.Fatalf("COMMAND COUNT = %d", n)
	}

	info, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("INFO: %v", err)
	}
	if !strings.Contains(info, "# Server") {
		t.Fatalf("INFO body: %q", info)
	}
}

func TestGoRedisClientUnknownCommand(t *testing.T) {
	addr := startGoRedisServer(t)
	rdb := newGoRedisClient(t, addr)
	ctx := context.Background()

	err := rdb.Do(ctx, "NOSUCH").Err()
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("got %v", err)
	}
}
