// Package server implements the TCP RESP server.
package server

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/metrics"
	"cache-custom/internal/protocol"
)

// Options configures connection limits, TLS (M7), and observability (M8).
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
	// Metrics optional command/connection counters.
	Metrics *metrics.Collector
	// Logger structured logger; nil uses slog.Default().
	Logger *slog.Logger
	// LogCommands when true, logs each command at Info with tenant and conn id.
	LogCommands bool
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
	// connSeq assigns ConnIDs.
	connSeq atomic.Uint64
	// ready is set true after Serve starts accepting.
	ready atomic.Bool
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
	s.ready.Store(true)
	defer s.ready.Store(false)

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
			s.logger().Warn("max clients reached", "remote", conn.RemoteAddr().String())
			_ = writeConnError(conn, "ERR max number of clients reached")
			_ = conn.Close()
			continue
		}
		if s.Opts.Metrics != nil {
			s.Opts.Metrics.ConnAccepted()
		}
		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			defer s.releaseClient()
			if s.Opts.Metrics != nil {
				defer s.Opts.Metrics.ConnClosed()
			}
			s.handleConn(conn)
		}()
	}
}

// Ready reports whether the server is accepting connections (for /readyz).
func (s *Server) Ready() bool {
	return s.ready.Load()
}

func (s *Server) logger() *slog.Logger {
	if s.Opts.Logger != nil {
		return s.Opts.Logger
	}
	return slog.Default()
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
	connID := s.connSeq.Add(1)
	remote := conn.RemoteAddr().String()
	ctx := &command.Context{
		Writer:     writer,
		ConnID:     connID,
		RemoteAddr: remote,
	}
	s.logger().Info("connection accepted",
		"conn_id", connID,
		"remote", remote,
	)
	defer func() {
		tenantName := ""
		if ctx.Tenant != nil {
			tenantName = ctx.Tenant.Name
		}
		s.logger().Info("connection closed",
			"conn_id", connID,
			"remote", remote,
			"tenant", tenantName,
		)
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
				s.logger().Warn("protocol error",
					"conn_id", connID,
					"remote", remote,
					"err", err.Error(),
				)
			}
			return
		}

		cmdName := ""
		if len(args) > 0 {
			cmdName = args[0]
		}
		start := time.Now()
		ctx.Multi = nil
		reply := s.Registry.Dispatch(ctx, args)
		dur := time.Since(start)

		tenantName := ""
		if ctx.Tenant != nil {
			tenantName = ctx.Tenant.Name
		}
		isErr := reply.Type == protocol.Error
		if s.Opts.Metrics != nil {
			s.Opts.Metrics.ObserveCommand(tenantName, cmdName, dur, isErr)
		}
		if s.Opts.LogCommands {
			s.logger().Info("command",
				"conn_id", connID,
				"remote", remote,
				"tenant", tenantName,
				"cmd", cmdName,
				"dur_us", dur.Microseconds(),
				"error", isErr,
			)
		} else if isErr && cmdName != "" {
			// Always log error replies at debug-friendly Warn without full command dump noise for PING.
			s.logger().Debug("command error",
				"conn_id", connID,
				"tenant", tenantName,
				"cmd", cmdName,
				"err", reply.Str,
			)
		}

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
