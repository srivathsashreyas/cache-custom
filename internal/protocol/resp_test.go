package protocol

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

func encodeString(t *testing.T, v Value) string {
	t.Helper()
	var buf bytes.Buffer
	if err := Write(&buf, v); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return buf.String()
}

func readFrom(t *testing.T, s string) Value {
	t.Helper()
	v, err := Read(bufio.NewReader(strings.NewReader(s)))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return v
}

func TestWriteSimpleString(t *testing.T) {
	got := encodeString(t, SimpleStringValue("OK"))
	if got != "+OK\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteError(t *testing.T) {
	got := encodeString(t, ErrorValue("ERR unknown command"))
	if got != "-ERR unknown command\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteInteger(t *testing.T) {
	got := encodeString(t, IntegerValue(42))
	if got != ":42\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteBulkString(t *testing.T) {
	got := encodeString(t, BulkStringValue("foo"))
	if got != "$3\r\nfoo\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteEmptyBulkString(t *testing.T) {
	got := encodeString(t, BulkStringValue(""))
	if got != "$0\r\n\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteNullBulk(t *testing.T) {
	got := encodeString(t, NullValue())
	if got != "$-1\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteArray(t *testing.T) {
	v := ArrayValue(BulkStringValue("PING"), BulkStringValue("hi"))
	got := encodeString(t, v)
	want := "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestReadSimpleString(t *testing.T) {
	v := readFrom(t, "+PONG\r\n")
	if v.Type != SimpleString || v.Str != "PONG" {
		t.Fatalf("got %+v", v)
	}
}

func TestReadError(t *testing.T) {
	v := readFrom(t, "-ERR boom\r\n")
	if v.Type != Error || v.Str != "ERR boom" {
		t.Fatalf("got %+v", v)
	}
}

func TestReadInteger(t *testing.T) {
	v := readFrom(t, ":-7\r\n")
	if v.Type != Integer || v.Int != -7 {
		t.Fatalf("got %+v", v)
	}
}

func TestReadBulkString(t *testing.T) {
	v := readFrom(t, "$5\r\nhello\r\n")
	if v.Type != BulkString || v.Str != "hello" {
		t.Fatalf("got %+v", v)
	}
}

func TestReadBulkStringWithEmbeddedCRLFContent(t *testing.T) {
	// Length-prefixed payload may contain CR/LF bytes as data.
	payload := "a\r\nb"
	raw := "$4\r\n" + payload + "\r\n"
	v := readFrom(t, raw)
	if v.Type != BulkString || v.Str != payload {
		t.Fatalf("got %+v", v)
	}
}

func TestReadNullBulk(t *testing.T) {
	v := readFrom(t, "$-1\r\n")
	if v.Type != Null {
		t.Fatalf("got %+v", v)
	}
}

func TestReadArray(t *testing.T) {
	raw := "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	v := readFrom(t, raw)
	if v.Type != Array || len(v.Array) != 2 {
		t.Fatalf("got %+v", v)
	}
	if v.Array[0].Str != "GET" || v.Array[1].Str != "k" {
		t.Fatalf("got %+v", v)
	}
}

func TestRoundTrip(t *testing.T) {
	orig := ArrayValue(
		BulkStringValue("SET"),
		BulkStringValue("key"),
		BulkStringValue("val\x00ue"),
		IntegerValue(1),
	)
	var buf bytes.Buffer
	if err := Write(&buf, orig); err != nil {
		t.Fatal(err)
	}
	got, err := Read(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != Array || len(got.Array) != 4 {
		t.Fatalf("got %+v", got)
	}
	if got.Array[2].Str != "val\x00ue" {
		t.Fatalf("binary bulk mismatch: %q", got.Array[2].Str)
	}
}

func TestReadCommandRESP(t *testing.T) {
	raw := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	args, err := ReadCommand(bufio.NewReader(strings.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || args[0] != "ECHO" || args[1] != "hello" {
		t.Fatalf("got %v", args)
	}
}

func TestReadCommandInline(t *testing.T) {
	args, err := ReadCommand(bufio.NewReader(strings.NewReader("PING\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 || args[0] != "PING" {
		t.Fatalf("got %v", args)
	}
}

func TestReadCommandInlineWithArgs(t *testing.T) {
	args, err := ReadCommand(bufio.NewReader(strings.NewReader("ECHO hello world\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 3 || args[0] != "ECHO" || args[1] != "hello" || args[2] != "world" {
		t.Fatalf("got %v", args)
	}
}

func TestPipelineTwoCommands(t *testing.T) {
	raw := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$1\r\nx\r\n"
	r := bufio.NewReader(strings.NewReader(raw))
	a1, err := ReadCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := ReadCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	if a1[0] != "PING" || a2[0] != "ECHO" || a2[1] != "x" {
		t.Fatalf("got %v %v", a1, a2)
	}
}

func TestMalformedMissingCRLF(t *testing.T) {
	_, err := Read(bufio.NewReader(strings.NewReader("+OK\n")))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMalformedBulkLength(t *testing.T) {
	_, err := Read(bufio.NewReader(strings.NewReader("$abc\r\n")))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMalformedTruncatedBulk(t *testing.T) {
	_, err := Read(bufio.NewReader(strings.NewReader("$5\r\nhi\r\n")))
	if err == nil {
		t.Fatal("expected error for short bulk payload")
	}
}

func TestEmptyCommandArray(t *testing.T) {
	_, err := ReadCommand(bufio.NewReader(strings.NewReader("*0\r\n")))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUnexpectedEOF(t *testing.T) {
	_, err := Read(bufio.NewReader(strings.NewReader("")))
	if err != io.EOF {
		t.Fatalf("got %v", err)
	}
}
