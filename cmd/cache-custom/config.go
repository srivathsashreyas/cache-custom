package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cache-custom/internal/persist"
	"cache-custom/internal/store"
)

// TenantConfig is one tenant entry.
type TenantConfig struct {
	Name             string
	Password         string
	AppId            uint64
	MaxMemory        uint64
	EvictionPolicy   string
	MaxTTL           int64
	ShardCount       int
	ShardingStrategy int
	// MaxClients limits simultaneous AUTH-bound connections for this tenant (0 = unlimited).
	MaxClients int
	Disabled   bool
}

// PersistConfig is server-level durability settings.
type PersistConfig struct {
	Mode                string // none | snapshot | aof | snapshot+aof
	Dir                 string
	AOFFsync            string // always | everysec | no
	SnapshotIntervalSec int    // 0 = manual only
}

// SecurityConfig is M7 security and connection controls.
// Profile: "local" | "protected" (default "protected" for object-form config).
type SecurityConfig struct {
	// Profile is "local" or "protected". Empty defaults to "protected" for object form,
	// and "local" for bare tenant-array config (dev-friendly).
	Profile string
	// RequireAuth when set overrides profile default. nil = use profile default.
	// local default: false (auto-bind first tenant); protected default: true.
	RequireAuth *bool
	// MaxClients global connection limit (0 = unlimited).
	MaxClients int
	// IdleTimeoutSec closes idle connections (0 = none).
	IdleTimeoutSec int
	// DenyCommands upper/lower command names to disable (e.g. "FLUSHDB", "SAVE").
	DenyCommands []string
	// TLS
	TLSCertFile string
	TLSKeyFile  string
	// TLSClientCA optional PEM file; when set, clients must present a cert signed by this CA.
	TLSClientCA string
	// TLSMinVersion e.g. "1.2" or "1.3" (default 1.2).
	TLSMinVersion string
}

// ObservabilityConfig is M8 ops surface (metrics HTTP, logging).
type ObservabilityConfig struct {
	// MetricsAddr is the HTTP listen address for /metrics, /healthz, /readyz.
	// Empty disables the HTTP ops server.
	MetricsAddr string
	// LogJSON when true uses JSON slog to stdout.
	LogJSON bool
	// LogLevel is the minimum level: debug, info, warn, error (default info).
	LogLevel string
	// LogCommands when true logs every command with conn_id and tenant.
	LogCommands bool
}

// ServerConfig is the on-disk config file (object form).
type ServerConfig struct {
	Security      SecurityConfig
	Persistence   PersistConfig
	Observability ObservabilityConfig
	Tenants       []TenantConfig
}

// Config is an alias used by older call sites (tenant only).
type Config = TenantConfig

func readConfig(path string) (ServerConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ServerConfig{}, err
	}
	// Backward compatible: bare tenant array → local profile, no persistence.
	// AUTH still required (historical behavior); set RequireAuth false in object form for auto-bind.
	var arr []TenantConfig
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
		reqAuth := true
		sc := ServerConfig{
			Tenants:     arr,
			Persistence: PersistConfig{Mode: "none"},
			Security: SecurityConfig{
				Profile:     "local",
				RequireAuth: &reqAuth,
			},
		}
		if err := validate(sc); err != nil {
			return ServerConfig{}, err
		}
		// MaxClients caps applied in main after logger is configured.
		return sc, nil
	}
	var sc ServerConfig
	if err := json.Unmarshal(raw, &sc); err != nil {
		return ServerConfig{}, err
	}
	if sc.Security.Profile == "" {
		sc.Security.Profile = "protected"
	}
	if err := validate(sc); err != nil {
		return ServerConfig{}, err
	}
	return sc, nil
}

// capTenantMaxClients clamps each tenant's MaxClients to Security.MaxClients when the
// global limit is set and a tenant requests more. Returns human-readable warnings
// (caller should log them). Tenant MaxClients 0 (unlimited) is left unchanged.
func capTenantMaxClients(sc *ServerConfig) []string {
	global := sc.Security.MaxClients
	if global <= 0 {
		return nil
	}
	var warnings []string
	for i := range sc.Tenants {
		t := &sc.Tenants[i]
		if t.MaxClients <= 0 {
			continue // unlimited at tenant layer; global still caps TCP accepts
		}
		if t.MaxClients > global {
			warnings = append(warnings, fmt.Sprintf(
				"tenant %q MaxClients=%d exceeds Security.MaxClients=%d; capping tenant MaxClients to %d (tenant connections cannot exceed the global limit)",
				t.Name, t.MaxClients, global, global,
			))
			t.MaxClients = global
		}
	}
	return warnings
}

// parseLogLevel maps config strings to slog levels. Empty defaults to info.
func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid Observability.LogLevel %q (want debug|info|warn|error)", s)
	}
}

func validate(sc ServerConfig) error {
	if len(sc.Tenants) == 0 {
		return errors.New("at least one tenant required")
	}
	if sc.Persistence.Mode != "" {
		if _, ok := persist.ParseMode(sc.Persistence.Mode); !ok {
			return errors.New("invalid Persistence.Mode")
		}
	}
	prof := strings.ToLower(sc.Security.Profile)
	if prof != "" && prof != "local" && prof != "protected" {
		return errors.New(`Security.Profile must be "local" or "protected"`)
	}
	cert, key := sc.Security.TLSCertFile, sc.Security.TLSKeyFile
	if (cert == "") != (key == "") {
		return errors.New("Security.TLSCertFile and TLSKeyFile must both be set or both empty")
	}
	if sc.Security.MaxClients < 0 {
		return errors.New("Security.MaxClients must be >= 0")
	}
	if sc.Security.IdleTimeoutSec < 0 {
		return errors.New("Security.IdleTimeoutSec must be >= 0")
	}
	if _, err := parseLogLevel(sc.Observability.LogLevel); err != nil {
		return err
	}
	for i := range sc.Tenants {
		c := &sc.Tenants[i]
		if c.Name == "" {
			return errors.New("each tenant requires Name")
		}
		if c.Password == "" {
			return errors.New("each tenant requires Password")
		}
		if c.EvictionPolicy != "" {
			if _, ok := store.ParseEvictionPolicy(c.EvictionPolicy); !ok {
				return errors.New("invalid EvictionPolicy for tenant " + c.Name)
			}
		}
		if c.ShardCount < 0 {
			return errors.New("ShardCount must be >= 0")
		}
		if c.ShardingStrategy != 0 && (c.ShardingStrategy < 1 || c.ShardingStrategy > 3) {
			return errors.New("ShardingStrategy must be 1, 2, or 3")
		}
		if c.MaxClients < 0 {
			return errors.New("MaxClients must be >= 0 for tenant " + c.Name)
		}
	}
	return nil
}

func evictionFromConfig(c TenantConfig) store.EvictionPolicy {
	p, _ := store.ParseEvictionPolicy(c.EvictionPolicy)
	return p
}

func persistConfigFrom(pc PersistConfig) persist.Config {
	mode, _ := persist.ParseMode(pc.Mode)
	cfg := persist.Config{
		Mode:  mode,
		Dir:   pc.Dir,
		Fsync: persist.ParseFsync(pc.AOFFsync),
	}
	if pc.SnapshotIntervalSec > 0 {
		cfg.SnapshotInterval = time.Duration(pc.SnapshotIntervalSec) * time.Second
	}
	return cfg
}

// resolveRequireAuth returns whether AUTH is required for non-allowlist commands.
func resolveRequireAuth(sec SecurityConfig) bool {
	if sec.RequireAuth != nil {
		return *sec.RequireAuth
	}
	// protected → true; local → false (auto-bind first tenant).
	return strings.ToLower(sec.Profile) != "local"
}

// loadTLSConfig builds a tls.Config from security settings. nil if TLS disabled.
func loadTLSConfig(sec SecurityConfig) (*tls.Config, error) {
	if sec.TLSCertFile == "" || sec.TLSKeyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(sec.TLSCertFile, sec.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert/key: %w", err)
	}
	minVer := uint16(tls.VersionTLS12)
	switch strings.TrimSpace(sec.TLSMinVersion) {
	case "", "1.2", "TLS1.2", "tls1.2":
		minVer = tls.VersionTLS12
	case "1.3", "TLS1.3", "tls1.3":
		minVer = tls.VersionTLS13
	default:
		return nil, fmt.Errorf("unsupported Security.TLSMinVersion %q", sec.TLSMinVersion)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVer,
	}
	if sec.TLSClientCA != "" {
		pem, err := os.ReadFile(sec.TLSClientCA)
		if err != nil {
			return nil, fmt.Errorf("read TLSClientCA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("TLSClientCA: no certificates parsed")
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
