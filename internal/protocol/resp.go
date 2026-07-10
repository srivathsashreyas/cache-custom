// Package protocol implements Redis Serialization Protocol (RESP2).
package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Type is the RESP2 value kind.
type Type int

const (
	SimpleString Type = iota
	Error
	Integer
	BulkString
	Array
	Null
)

// Value is a RESP2 value.
type Value struct {
	Type  Type
	Str   string  // SimpleString, Error, BulkString
	Int   int64   // Integer
	Array []Value // Array
}

// SimpleStringValue returns a +simple string.
func SimpleStringValue(s string) Value {
	return Value{Type: SimpleString, Str: s}
}

// ErrorValue returns a -error.
func ErrorValue(msg string) Value {
	return Value{Type: Error, Str: msg}
}

// IntegerValue returns a :integer.
func IntegerValue(n int64) Value {
	return Value{Type: Integer, Int: n}
}

// BulkStringValue returns a $bulk string.
func BulkStringValue(s string) Value {
	return Value{Type: BulkString, Str: s}
}

// NullValue returns a null bulk string ($-1).
func NullValue() Value {
	return Value{Type: Null}
}

// ArrayValue returns a *array.
func ArrayValue(elems ...Value) Value {
	return Value{Type: Array, Array: elems}
}

// Write encodes v in RESP2 form to w.
// Types: +simple, -error, :int, $bulk / $-1 null, *array.
func Write(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleString:
		_, err := fmt.Fprintf(w, "+%s\r\n", v.Str)
		return err
	case Error:
		_, err := fmt.Fprintf(w, "-%s\r\n", v.Str)
		return err
	case Integer:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Int)
		return err
	case BulkString:
		// Length prefix so payload may contain CR/LF/NUL safely.
		_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		return err
	case Null:
		_, err := io.WriteString(w, "$-1\r\n")
		return err
	case Array:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Array)); err != nil {
			return err
		}
		for _, elem := range v.Array {
			if err := Write(w, elem); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("protocol: unknown value type %d", v.Type)
	}
}

// Read decodes one RESP2 value from r.
// Non-type-prefix bytes are treated as Redis inline protocol (see readInline).
func Read(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+':
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return SimpleStringValue(line), nil
	case '-':
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return ErrorValue(line), nil
	case ':':
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("protocol: invalid integer %q: %w", line, err)
		}
		return IntegerValue(n), nil
	case '$':
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, fmt.Errorf("protocol: invalid bulk length %q: %w", line, err)
		}
		if n == -1 {
			return NullValue(), nil
		}
		if n < 0 {
			return Value{}, fmt.Errorf("protocol: invalid bulk length %d", n)
		}
		// Read exactly n bytes of payload plus the trailing CRLF (not part of the value).
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Value{}, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return Value{}, fmt.Errorf("protocol: bulk string missing CRLF trailer")
		}
		return BulkStringValue(string(buf[:n])), nil
	case '*':
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			return Value{}, fmt.Errorf("protocol: invalid array length %q: %w", line, err)
		}
		if n == -1 {
			return NullValue(), nil
		}
		if n < 0 {
			return Value{}, fmt.Errorf("protocol: invalid array length %d", n)
		}
		elems := make([]Value, n)
		for i := 0; i < n; i++ {
			elems[i], err = Read(r)
			if err != nil {
				return Value{}, err
			}
		}
		return ArrayValue(elems...), nil
	default:
		// Not a RESP type tag: put the byte back and parse an inline command line.
		if err := r.UnreadByte(); err != nil {
			return Value{}, err
		}
		return readInline(r)
	}
}

// ReadCommand reads one client command as a list of string arguments.
// Accepts a RESP array of bulk strings (normal clients) or an inline command line.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	v, err := Read(r)
	if err != nil {
		return nil, err
	}

	switch v.Type {
	case Array:
		if len(v.Array) == 0 {
			return nil, fmt.Errorf("protocol: empty command array")
		}
		args := make([]string, len(v.Array))
		for i, elem := range v.Array {
			switch elem.Type {
			case BulkString, SimpleString:
				args[i] = elem.Str
			case Integer:
				args[i] = strconv.FormatInt(elem.Int, 10)
			default:
				return nil, fmt.Errorf("protocol: command argument %d has unsupported type", i)
			}
		}
		return args, nil
	case SimpleString, BulkString:
		// Single-token inline already parsed as one string value is unusual;
		// inline path returns Array. Treat as single-arg command.
		if v.Str == "" {
			return nil, fmt.Errorf("protocol: empty command")
		}
		return []string{v.Str}, nil
	default:
		return nil, fmt.Errorf("protocol: expected command array, got type %d", v.Type)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("protocol: line missing CRLF")
	}
	return line[:len(line)-2], nil
}

// readInline parses Redis inline protocol: COMMAND arg1 arg2\r\n
func readInline(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	if line == "" {
		return Value{}, fmt.Errorf("protocol: empty inline command")
	}
	parts := splitArgs(line)
	elems := make([]Value, len(parts))
	for i, p := range parts {
		elems[i] = BulkStringValue(p)
	}
	return ArrayValue(elems...), nil
}

func splitArgs(line string) []string {
	var parts []string
	var cur []byte
	inSpace := true
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == ' ' || c == '\t' {
			if !inSpace {
				parts = append(parts, string(cur))
				cur = cur[:0]
				inSpace = true
			}
			continue
		}
		inSpace = false
		cur = append(cur, c)
	}
	if !inSpace {
		parts = append(parts, string(cur))
	}
	return parts
}
