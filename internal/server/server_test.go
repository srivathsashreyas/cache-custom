package server

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/protocol"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	reg := command.NewRegistry()
	command.RegisterDefaults(reg, nil)
	s := New("127.0.0.1:0", reg)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() { _ = s.Close() })

	// Brief wait for accept loop.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return s, addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server did not accept connections")
	return s, addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeRaw(t *testing.T, c net.Conn, s string) {
	t.Helper()
	if _, err := io.WriteString(c, s); err != nil {
		t.Fatal(err)
	}
}

func readValue(t *testing.T, c net.Conn) protocol.Value {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	v, err := protocol.Read(bufio.NewReader(c))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestServerPingRESP(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$4\r\nPING\r\n")
	v := readValue(t, c)
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestServerPingInline(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "PING\r\n")
	v := readValue(t, c)
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestServerEcho(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
	v := readValue(t, c)
	if v.Type != protocol.BulkString || v.Str != "hey" {
		t.Fatalf("got %+v", v)
	}
}

func TestServerPipeline(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$1\r\nz\r\n")

	br := bufio.NewReader(c)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	v1, err := protocol.Read(br)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := protocol.Read(br)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Str != "PONG" || v2.Str != "z" {
		t.Fatalf("got %+v %+v", v1, v2)
	}
}

func TestServerQuit(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$4\r\nQUIT\r\n")
	v := readValue(t, c)
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("got %+v", v)
	}
	// Connection should close; further reads hit EOF.
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 8)
	_, err := c.Read(buf)
	if err == nil {
		t.Fatal("expected EOF after QUIT")
	}
}

func TestServerUnknownCommand(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$6\r\nNOSUCH\r\n")
	v := readValue(t, c)
	if v.Type != protocol.Error || !strings.Contains(v.Str, "unknown command") {
		t.Fatalf("got %+v", v)
	}
}

func TestServerMalformedDoesNotCrash(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "$999\r\nshort\r\n")
	// Server may reply with protocol error then close, or just close.
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	br := bufio.NewReader(c)
	_, _ = protocol.Read(br)

	// Fresh connection still works.
	c2 := dial(t, addr)
	writeRaw(t, c2, "PING\r\n")
	v := readValue(t, c2)
	if v.Str != "PONG" {
		t.Fatalf("server dead after malformed input: %+v", v)
	}
}

func TestServerInfo(t *testing.T) {
	_, addr := startTestServer(t)
	c := dial(t, addr)
	writeRaw(t, c, "*1\r\n$4\r\nINFO\r\n")
	v := readValue(t, c)
	if v.Type != protocol.BulkString || !strings.Contains(v.Str, "# Server") {
		t.Fatalf("got %+v", v)
	}
}

func TestServerAuthIsolation(t *testing.T) {
	tenants, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, Strategy: store.StrategySteal, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tenants.Close)

	reg := command.NewRegistry()
	command.RegisterDefaults(reg, nil)
	command.RegisterAuth(reg, tenants)
	command.RegisterStringCommands(reg)

	s := New("127.0.0.1:0", reg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })

	// Wait for listen
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	c1 := dial(t, addr)
	writeRaw(t, c1, "*3\r\n$4\r\nAUTH\r\n$4\r\nApp1\r\n$2\r\np1\r\n")
	if v := readValue(t, c1); v.Str != "OK" {
		t.Fatalf("auth1 %+v", v)
	}
	writeRaw(t, c1, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nva1\r\n")
	if v := readValue(t, c1); v.Str != "OK" {
		t.Fatalf("set1 %+v", v)
	}

	c2 := dial(t, addr)
	writeRaw(t, c2, "*3\r\n$4\r\nAUTH\r\n$4\r\nApp2\r\n$2\r\np2\r\n")
	_ = readValue(t, c2)
	writeRaw(t, c2, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nva2\r\n")
	_ = readValue(t, c2)

	writeRaw(t, c1, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	v1 := readValue(t, c1)
	writeRaw(t, c2, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	v2 := readValue(t, c2)
	if v1.Str != "va1" || v2.Str != "va2" {
		t.Fatalf("isolation v1=%+v v2=%+v", v1, v2)
	}

	// Unauthenticated GET
	c3 := dial(t, addr)
	writeRaw(t, c3, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
	v3 := readValue(t, c3)
	if v3.Type != protocol.Error || v3.Str != "NOAUTH Authentication required." {
		t.Fatalf("noauth %+v", v3)
	}
}
