// Package server implements the TCP RESP server.
package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/protocol"
)

// Options configures connection limits and TLS (M7).
type Options struct {
	// TLSConfig when non-nil wraps the listener with TLS.
	TLSConfig *tls.Config
	// MaxClients is the global connection limit (0 = unlimited).
	MaxClients int
	// IdleTimeout closes connections with no command for this long (0 = none).
	// Also used as per-read deadline when set.
	IdleTimeout time.Duration
	// ReadTimeout optional per-read deadline; if IdleTimeout is set it takes precedence.
	ReadTimeout time.Duration
}

// Server accepts TCP connections and serves RESP commands.
type Server struct {
	Addr     string
	Registry *command.Registry
	Opts     Options

	// ReadTimeout optional per-read deadline (0 = none). Deprecated: prefer Opts.
	ReadTimeout time.Duration

	ln net.Listener
	wg sync.WaitGroup

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}

	// clientCount is the number of currently open client connections.
	clientCount int64
}

// New creates a server with the given listen address and command registry.
func New(addr string, reg *command.Registry) *Server {
	return &Server{
		Addr:     addr,
		Registry: reg,
		conns:    make(map[net.Conn]struct{}),
	}
}

// NewWithOptions creates a server with security/connection options.
func NewWithOptions(addr string, reg *command.Registry, opts Options) *Server {
	s := New(addr, reg)
	s.Opts = opts
	if opts.ReadTimeout > 0 {
		s.ReadTimeout = opts.ReadTimeout
	}
	return s
}

// ListenAndServe listens on s.Addr and blocks until the listener fails or Close.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	if s.Opts.TLSConfig != nil {
		ln = tls.NewListener(ln, s.Opts.TLSConfig)
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln.
// Each client runs in its own goroutine so one slow connection cannot block others.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		if !s.tryAddClient(conn) {
			// Redis-style: refuse when at max clients.
			_ = writeConnError(conn, "ERR max number of clients reached")
			_ = conn.Close()
			continue
		}
		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			defer s.releaseClient()
			s.handleConn(conn)
		}()
	}
}

// tryAddClient increments the global client count if under MaxClients.
func (s *Server) tryAddClient(conn net.Conn) bool {
	max := s.Opts.MaxClients
	if max <= 0 {
		atomic.AddInt64(&s.clientCount, 1)
		return true
	}
	for {
		n := atomic.LoadInt64(&s.clientCount)
		if n >= int64(max) {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.clientCount, n, n+1) {
			return true
		}
	}
}

func (s *Server) releaseClient() {
	atomic.AddInt64(&s.clientCount, -1)
}

// ClientCount returns the number of open client connections.
func (s *Server) ClientCount() int {
	n := atomic.LoadInt64(&s.clientCount)
	if n < 0 {
		return 0
	}
	return int(n)
}

func writeConnError(conn net.Conn, msg string) error {
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	return protocol.Write(conn, protocol.ErrorValue(msg))
}

// Close stops accepting and closes active connections.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	return err
}

func (s *Server) track(c net.Conn) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	_ = c.Close()
}

// connWriter serializes command replies and Pub/Sub push messages on one connection.
type connWriter struct {
	mu sync.Mutex
	bw *bufio.Writer
}

func (w *connWriter) WriteValue(v protocol.Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := protocol.Write(w.bw, v); err != nil {
		return err
	}
	return w.bw.Flush()
}

// handleConn is the per-connection request loop.
func (s *Server) handleConn(conn net.Conn) {
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	writer := &connWriter{bw: bw}
	ctx := &command.Context{Writer: writer}
	defer func() {
		if ctx.PubSub != nil {
			ctx.PubSub.Close()
		}
		command.ReleaseTenantConn(ctx)
	}()

	readTimeout := s.effectiveReadTimeout()

	for {
		if readTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		}

		args, err := protocol.ReadCommand(br)
		if err != nil {
			if !isConnClosed(err) {
				_ = writer.WriteValue(protocol.ErrorValue("ERR protocol error: " + err.Error()))
				log.Printf("connection %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		ctx.Multi = nil
		reply := s.Registry.Dispatch(ctx, args)
		if len(ctx.Multi) > 0 {
			for _, m := range ctx.Multi {
				if err := writer.WriteValue(m); err != nil {
					return
				}
			}
			ctx.Multi = nil
		} else {
			if err := writer.WriteValue(reply); err != nil {
				return
			}
		}
		if ctx.Quit {
			return
		}
	}
}

func (s *Server) effectiveReadTimeout() time.Duration {
	if s.Opts.IdleTimeout > 0 {
		return s.Opts.IdleTimeout
	}
	if s.Opts.ReadTimeout > 0 {
		return s.Opts.ReadTimeout
	}
	return s.ReadTimeout
}

// ListenerAddr returns the bound address (useful after Listen on :0).
func (s *Server) ListenerAddr() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return "", fmt.Errorf("server: not listening")
	}
	return s.ln.Addr().String(), nil
}

func isConnClosed(err error) bool {
	if err == nil || err == io.EOF {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	// Idle/read timeout: treat as closed.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Err != nil {
		msg := op.Err.Error()
		return msg == "use of closed network connection" || msg == "connection reset by peer"
	}
	return false
}
