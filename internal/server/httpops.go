package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"cache-custom/internal/metrics"
	"cache-custom/internal/tenant"
)

// HTTPOps serves /metrics, /healthz, and /readyz for operators and K8s probes (M8/M9).
type HTTPOps struct {
	Addr       string
	Metrics    *metrics.Collector
	Tenants    *tenant.Registry
	Connected  func() int
	MaxClients int
	// Ready is true when the RESP server is accepting work (optional; prefer ReadyFunc).
	Ready *atomic.Bool
	// ReadyFunc when set is used for /readyz (e.g. server.Ready).
	ReadyFunc func() bool

	srv *http.Server
}

// ListenAndServe starts the HTTP ops server (blocking). Returns http.ErrServerClosed on shutdown.
func (h *HTTPOps) ListenAndServe() error {
	if h == nil || h.Addr == "" {
		return fmt.Errorf("server: HTTPOps addr required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/readyz", h.handleReadyz)
	h.srv = &http.Server{
		Addr:              h.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return h.srv.ListenAndServe()
}

// Serve on an existing listener (tests).
func (h *HTTPOps) Serve(ln net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/readyz", h.handleReadyz)
	h.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return h.srv.Serve(ln)
}

// Shutdown gracefully stops the HTTP server.
func (h *HTTPOps) Shutdown(ctx context.Context) error {
	if h == nil || h.srv == nil {
		return nil
	}
	return h.srv.Shutdown(ctx)
}

func (h *HTTPOps) handleHealthz(w http.ResponseWriter, r *http.Request) {
	// Liveness: process is up.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (h *HTTPOps) handleReadyz(w http.ResponseWriter, r *http.Request) {
	// Readiness: RESP server marked ready (see M9 probe split).
	ok := true
	if h.ReadyFunc != nil {
		ok = h.ReadyFunc()
	} else if h.Ready != nil {
		ok = h.Ready.Load()
	}
	if !ok {
		http.Error(w, "not ready\n", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (h *HTTPOps) handleMetrics(w http.ResponseWriter, r *http.Request) {
	connected := 0
	if h.Connected != nil {
		connected = h.Connected()
	}
	var stats []metrics.TenantStat
	if h.Tenants != nil {
		for _, t := range h.Tenants.All() {
			used, max, keys, hits, misses, evictions := t.DB.Stats()
			stats = append(stats, metrics.TenantStat{
				Name:       t.Name,
				UsedMemory: used,
				MaxMemory:  max,
				Keys:       keys,
				Hits:       hits,
				Misses:     misses,
				Evictions:  evictions,
				ConnCount:  t.ConnCount(),
			})
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	coll := h.Metrics
	if coll == nil {
		coll = metrics.New()
	}
	_ = coll.WritePrometheus(w, connected, h.MaxClients, stats)
}
