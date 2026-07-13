package command

import (
	"strings"
	"testing"

	"cache-custom/internal/protocol"
)

func newTestRegistry() *Registry {
	r := NewRegistry()
	RegisterDefaults(r, nil)
	return r
}

func TestPingNoArgs(t *testing.T) {
	r := newTestRegistry()
	ctx := &Context{}
	v := r.Dispatch(ctx, []string{"PING"})
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestPingCaseInsensitive(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"ping"})
	if v.Type != protocol.SimpleString || v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestPingWithMessage(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"PING", "hello"})
	if v.Type != protocol.BulkString || v.Str != "hello" {
		t.Fatalf("got %+v", v)
	}
}

func TestPingWrongArity(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"PING", "a", "b"})
	if v.Type != protocol.Error {
		t.Fatalf("got %+v", v)
	}
}

func TestEcho(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"ECHO", "world"})
	if v.Type != protocol.BulkString || v.Str != "world" {
		t.Fatalf("got %+v", v)
	}
}

func TestEchoWrongArity(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"ECHO"})
	if v.Type != protocol.Error {
		t.Fatalf("got %+v", v)
	}
}

func TestQuitSetsFlag(t *testing.T) {
	r := newTestRegistry()
	ctx := &Context{}
	v := r.Dispatch(ctx, []string{"QUIT"})
	if v.Type != protocol.SimpleString || v.Str != "OK" {
		t.Fatalf("got %+v", v)
	}
	if !ctx.Quit {
		t.Fatal("expected Quit flag")
	}
}

func TestUnknownCommand(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"FOOBAR"})
	if v.Type != protocol.Error || !strings.Contains(v.Str, "unknown command") {
		t.Fatalf("got %+v", v)
	}
}

func TestEmptyCommand(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, nil)
	if v.Type != protocol.Error {
		t.Fatalf("got %+v", v)
	}
}

func TestCommandList(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"COMMAND"})
	if v.Type != protocol.Array {
		t.Fatalf("got %+v", v)
	}
	found := map[string]bool{}
	for _, e := range v.Array {
		found[e.Str] = true
	}
	for _, name := range []string{"PING", "ECHO", "QUIT", "COMMAND", "INFO"} {
		if !found[name] {
			t.Fatalf("missing %s in %v", name, found)
		}
	}
}

func TestCommandCount(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"COMMAND", "COUNT"})
	// Defaults only (PING/ECHO/QUIT/COMMAND/INFO); string cmds registered separately.
	if v.Type != protocol.Integer || v.Int != 5 {
		t.Fatalf("got %+v", v)
	}
}

func TestInfoServer(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"INFO"})
	if v.Type != protocol.BulkString {
		t.Fatalf("got %+v", v)
	}
	if !strings.Contains(v.Str, "# Server") {
		t.Fatalf("body: %q", v.Str)
	}
}

func TestInfoUnknownSection(t *testing.T) {
	r := newTestRegistry()
	v := r.Dispatch(&Context{}, []string{"INFO", "nosuch"})
	if v.Type != protocol.BulkString || v.Str != "" {
		t.Fatalf("got %+v", v)
	}
}
