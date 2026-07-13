// Package server implements the TCP RESP server.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/protocol"
)

// Server accepts TCP connections and serves RESP commands.
type Server struct {
	Addr     string
	Registry *command.Registry
	// ReadTimeout optional per-read deadline (0 = none).
	ReadTimeout time.Duration

	ln net.Listener
	wg sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	conns    map[net.Conn]struct{}
}

// New creates a server with the given listen address and command registry.
func New(addr string, reg *command.Registry) *Server {
	return &Server{
		Addr:     addr,
		Registry: reg,
		conns:    make(map[net.Conn]struct{}),
	}
}

// ListenAndServe listens on s.Addr and blocks until the listener fails or Close.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
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
		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			s.handleConn(conn)
		}()
	}
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

// handleConn is the per-connection request loop.
// One bufio.Reader is reused so pipelined commands (multiple frames already in the buffer) work.
func (s *Server) handleConn(conn net.Conn) {
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	// Per-connection context so AUTH tenant binding persists across commands.
	ctx := &command.Context{}

	for {
		if s.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.ReadTimeout))
		}

		args, err := protocol.ReadCommand(br)
		if err != nil {
			// Normal disconnects are silent; true protocol errors get a best-effort -ERR then close.
			if !isConnClosed(err) {
				_ = protocol.Write(bw, protocol.ErrorValue("ERR protocol error: "+err.Error()))
				_ = bw.Flush()
				log.Printf("connection %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		reply := s.Registry.Dispatch(ctx, args)
		if err := protocol.Write(bw, reply); err != nil {
			return
		}
		if err := bw.Flush(); err != nil {
			return
		}
		// QUIT replies OK then drops the session (Redis-compatible).
		if ctx.Quit {
			return
		}
	}
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
	// net.OpError / partial close strings vary by platform.
	var op *net.OpError
	if errors.As(err, &op) && op.Err != nil {
		msg := op.Err.Error()
		return msg == "use of closed network connection" || msg == "connection reset by peer"
	}
	return false
}
