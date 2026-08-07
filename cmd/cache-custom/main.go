package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cache-custom/internal/command"
	"cache-custom/internal/metrics"
	"cache-custom/internal/persist"
	"cache-custom/internal/server"
	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func main() {
	addr := flag.String("addr", ":9001", "TCP listen address for RESP")
	configPath := flag.String("config", "config.json", "path to config file")
	metricsAddrFlag := flag.String("metrics-addr", "", "HTTP ops listen address (/metrics,/healthz,/readyz); overrides config when set")
	flag.Parse()

	// Bootstrap logger for config-load failures (before Observability is known).
	bootstrap := setupLogger(false, slog.LevelInfo)
	slog.SetDefault(bootstrap)

	sc, err := readConfig(*configPath)
	if err != nil {
		bootstrap.Error("failed to read config", "path", *configPath, "err", err.Error())
		os.Exit(1)
	}

	level, err := parseLogLevel(sc.Observability.LogLevel)
	if err != nil {
		bootstrap.Error("invalid log level", "err", err.Error())
		os.Exit(1)
	}
	logger := setupLogger(sc.Observability.LogJSON, level)
	slog.SetDefault(logger)

	// Re-apply caps so warnings go through the configured logger (readConfig already capped).
	for _, w := range capTenantMaxClients(&sc) {
		logger.Warn(w)
	}

	tenants, err := tenant.NewRegistry(toTenantConfigs(sc.Tenants))
	if err != nil {
		logger.Error("tenant registry", "err", err.Error())
		os.Exit(1)
	}
	defer tenants.Close()

	eng := persist.New(persistConfigFrom(sc.Persistence))
	if err := eng.Open(); err != nil {
		logger.Error("persist open", "err", err.Error())
		os.Exit(1)
	}
	defer eng.Close()

	if err := eng.Load(tenants); err != nil {
		logger.Error("persist load", "err", err.Error())
		os.Exit(1)
	}
	eng.AttachSinks(tenants)
	eng.StartPeriodicSnapshots(tenants)

	requireAuth := resolveRequireAuth(sc.Security)
	var defaultTenant *tenant.Tenant
	if !requireAuth {
		all := tenants.All()
		if len(all) > 0 {
			defaultTenant = all[0]
		}
	}
	pol := command.NewPolicy(requireAuth, defaultTenant, sc.Security.DenyCommands)

	coll := metrics.New()
	start := coll.StartTime()

	reg := command.NewRegistry()
	reg.SetPolicy(pol)
	command.RegisterDefaults(reg, tenants)
	command.RegisterAuth(reg, tenants)
	command.RegisterStringCommands(reg)
	command.RegisterHashCommands(reg)
	command.RegisterListCommands(reg)
	command.RegisterSetCommands(reg)
	command.RegisterZSetCommands(reg)
	command.RegisterPubSub(reg)
	command.RegisterPersist(reg, eng, tenants)
	command.RegisterTenantStats(reg, tenants, coll)

	tlsCfg, err := loadTLSConfig(sc.Security)
	if err != nil {
		logger.Error("tls config", "err", err.Error())
		os.Exit(1)
	}
	var idle time.Duration
	if sc.Security.IdleTimeoutSec > 0 {
		idle = time.Duration(sc.Security.IdleTimeoutSec) * time.Second
	}
	opts := server.Options{
		TLSConfig:   tlsCfg,
		MaxClients:  sc.Security.MaxClients,
		IdleTimeout: idle,
		Metrics:     coll,
		Logger:      logger,
		LogCommands: sc.Observability.LogCommands,
	}
	srv := server.NewWithOptions(*addr, reg, opts)

	profile := sc.Security.Profile
	if profile == "" {
		profile = "protected"
	}
	tlsOn := tlsCfg != nil

	reg.SetRuntime(&command.RuntimeInfo{
		Tenants:          tenants,
		PersistMode:      string(eng.Mode()),
		PersistDir:       sc.Persistence.Dir,
		AOFFsync:         sc.Persistence.AOFFsync,
		LastSaveUnix:     eng.LastSaveUnix,
		ConnectedClients: srv.ClientCount,
		MaxClients:       sc.Security.MaxClients,
		Addr:             *addr,
		StartTime:        start,
		TLSEnabled:       tlsOn,
		Profile:          profile,
		RequireAuth:      requireAuth,
		Metrics:          coll,
	})

	metricsAddr := sc.Observability.MetricsAddr
	if *metricsAddrFlag != "" {
		metricsAddr = *metricsAddrFlag
	}

	var httpOps *server.HTTPOps
	if metricsAddr != "" {
		httpOps = &server.HTTPOps{
			Addr:       metricsAddr,
			Metrics:    coll,
			Tenants:    tenants,
			Connected:  srv.ClientCount,
			MaxClients: sc.Security.MaxClients,
			ReadyFunc:  srv.Ready,
		}
		go func() {
			logger.Info("http ops listening", "addr", metricsAddr, "paths", "/metrics,/healthz,/readyz")
			if err := httpOps.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("http ops server failed", "err", err.Error())
			}
		}()
	}

	logger.Info("server starting",
		"addr", *addr,
		"tls", tlsOn,
		"profile", profile,
		"require_auth", requireAuth,
		"log_level", level.String(),
		"persist_mode", eng.Mode(),
		"persist_dir", sc.Persistence.Dir,
	)
	if sc.Security.MaxClients > 0 {
		logger.Info("max clients", "global", sc.Security.MaxClients)
	}
	if len(sc.Security.DenyCommands) > 0 {
		logger.Info("denied commands", "commands", sc.Security.DenyCommands)
	}
	for _, t := range tenants.All() {
		mc := 0
		if t.MaxClients > 0 {
			mc = t.MaxClients
		}
		logger.Info("tenant loaded",
			"tenant", t.Name,
			"app_id", t.AppID,
			"max_memory", t.MaxMemory,
			"strategy", int(t.Strategy),
			"policy", string(t.Policy),
			"shards", t.Shards,
			"max_clients", mc,
			"keys", t.DB.DBSize(),
		)
	}
	if !requireAuth && defaultTenant != nil {
		logger.Info("local profile auto-bind", "tenant", defaultTenant.Name)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("server stopped", "err", err.Error())
			os.Exit(1)
		}
	case sig := <-sigCh:
		logger.Info("shutdown signal", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if httpOps != nil {
			_ = httpOps.Shutdown(ctx)
		}
		_ = srv.Close()
	}
}

func setupLogger(jsonLogs bool, level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if jsonLogs {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}

func toTenantConfigs(cfgs []TenantConfig) []tenant.Config {
	out := make([]tenant.Config, 0, len(cfgs))
	for _, c := range cfgs {
		sc := c.ShardCount
		if sc < 1 {
			sc = 4
		}
		st := store.Strategy(c.ShardingStrategy)
		if st < store.StrategyGlobalTrack || st > store.StrategyShardBudget {
			st = store.StrategyGlobalTrack
		}
		var maxTTL time.Duration
		if c.MaxTTL > 0 {
			maxTTL = time.Duration(c.MaxTTL) * time.Second
		}
		out = append(out, tenant.Config{
			Name:       c.Name,
			Password:   c.Password,
			AppID:      c.AppId,
			MaxMemory:  c.MaxMemory,
			MaxTTL:     maxTTL,
			ShardCount: sc,
			Strategy:   st,
			Policy:     evictionFromConfig(c),
			MaxClients: c.MaxClients,
			Disabled:   c.Disabled,
		})
	}
	return out
}
