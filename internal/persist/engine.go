// Package persist implements snapshot and AOF durability for multi-tenant stores.
package persist

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

const (
	snapMagic   = "CCSN"
	snapVersion = uint32(1)
	snapFile    = "dump.ccs"
	aofFile     = "appendonly.aof"
)

// Config configures the persistence engine.
type Config struct {
	Mode               Mode
	Dir                string
	Fsync              FsyncPolicy
	SnapshotInterval   time.Duration // 0 = only manual SAVE/BGSAVE
}

// Engine coordinates snapshot/AOF for a tenant registry.
type Engine struct {
	cfg Config

	mu       sync.Mutex
	aof      *os.File
	lastSave atomic.Int64 // unix seconds
	saving   atomic.Bool

	// During AOF rewrite or hybrid SAVE, concurrent mutations dual-write into catchup
	// under the same mu as AOF appends (no extra global lock on the data plane).
	catchingUp bool
	catchup    bytes.Buffer
}

// New creates an engine; call Open then Load.
func New(cfg Config) *Engine {
	if cfg.Dir == "" {
		cfg.Dir = "data"
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeNone
	}
	if cfg.Fsync == "" {
		cfg.Fsync = FsyncEverySec
	}
	return &Engine{cfg: cfg}
}

// Mode returns the configured mode.
func (e *Engine) Mode() Mode { return e.cfg.Mode }

// LastSaveUnix returns unix time of last successful snapshot (0 if never).
func (e *Engine) LastSaveUnix() int64 { return e.lastSave.Load() }

// Open prepares directories and AOF file handle when needed.
func (e *Engine) Open() error {
	if e.cfg.Mode == ModeNone {
		return nil
	}
	if err := os.MkdirAll(e.cfg.Dir, 0o755); err != nil {
		return err
	}
	if e.cfg.Mode == ModeAOF || e.cfg.Mode == ModeSnapshotAndAOF {
		f, err := os.OpenFile(e.aofPath(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		e.aof = f
		if e.cfg.Fsync == FsyncEverySec {
			go e.fsyncLoop()
		}
	}
	return nil
}

func (e *Engine) fsyncLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		e.mu.Lock()
		if e.aof != nil {
			_ = e.aof.Sync()
		}
		e.mu.Unlock()
	}
}

// Close flushes AOF.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.aof != nil {
		_ = e.aof.Sync()
		err := e.aof.Close()
		e.aof = nil
		return err
	}
	return nil
}

func (e *Engine) snapPath() string { return filepath.Join(e.cfg.Dir, snapFile) }
func (e *Engine) aofPath() string  { return filepath.Join(e.cfg.Dir, aofFile) }

// TenantSink routes store mutations into the AOF with a fixed tenant name.
type TenantSink struct {
	Engine *Engine
	Tenant string
}

func (s *TenantSink) OnMutation(m store.Mutation) {
	if s.Engine == nil || s.Engine.cfg.Mode == ModeNone || s.Engine.cfg.Mode == ModeSnapshot {
		return
	}
	_ = s.Engine.appendAOF(s.Tenant, m)
}

type aofLine struct {
	Op     string   `json:"op"`
	Tenant string   `json:"tenant"`
	Key    string   `json:"key,omitempty"`
	Value  string   `json:"value,omitempty"`
	Field  string   `json:"field,omitempty"`
	Member string   `json:"member,omitempty"`
	Keys   []string `json:"keys,omitempty"`
	Args   []string `json:"args,omitempty"`
	Exp    int64    `json:"exp,omitempty"` // unix nano; 0 = none
}

func (e *Engine) appendAOF(tenantName string, m store.Mutation) error {
	line := aofLine{
		Op: m.Op, Tenant: tenantName, Key: m.Key, Value: m.Value,
		Field: m.Field, Member: m.Member, Keys: m.Keys, Args: m.Args,
	}
	if !m.ExpiresAt.IsZero() {
		line.Exp = m.ExpiresAt.UnixNano()
	}
	b, err := json.Marshal(line)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()
	// Live AOF (may be nil briefly during swap).
	if e.aof != nil {
		if _, err := e.aof.Write(b); err != nil {
			return err
		}
		if e.cfg.Fsync == FsyncAlways {
			if err := e.aof.Sync(); err != nil {
				return err
			}
		}
	}
	// Compaction window: also buffer so the new base+delta stays complete.
	if e.catchingUp {
		_, _ = e.catchup.Write(b)
	}
	return nil
}

// SnapshotFile is the on-disk multi-tenant snapshot layout (versioned + CRC32).
// Corruption policy: refuse load if magic/version/CRC invalid (entire file).

type snapTenant struct {
	Name string
	Keys []store.Record
}

// SaveSnapshot writes dump.ccs for snapshot/hybrid modes.
// Hybrid: dual-write AOF mutations into a catchup buffer for the whole export+write
// window, then replace AOF with catchup only (no silent drop of concurrent writes).
// AOF-only SAVE uses rewriteAOF with the same catchup pattern.
func (e *Engine) SaveSnapshot(reg *tenant.Registry) error {
	if e.cfg.Mode == ModeNone {
		return nil
	}
	// Single in-progress gate for SAVE/BGSAVE (covers snapshot, hybrid, and AOF rewrite).
	if !e.saving.CompareAndSwap(false, true) {
		return fmt.Errorf("ERR Background save already in progress")
	}
	defer e.saving.Store(false)

	// One catchup window for AOF rewrite and hybrid SAVE (rewriteAOF does not start its own).
	if e.cfg.Mode == ModeAOF || e.cfg.Mode == ModeSnapshotAndAOF {
		e.beginCatchup()
	}

	if e.cfg.Mode == ModeAOF {
		return e.rewriteAOF(reg)
	}

	if err := os.MkdirAll(e.cfg.Dir, 0o755); err != nil {
		e.abortCatchup()
		return err
	}

	tmp := e.snapPath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		e.abortCatchup()
		return err
	}

	tenants := reg.All()
	payload := make([]snapTenant, 0, len(tenants))
	for _, t := range tenants {
		payload = append(payload, snapTenant{Name: t.Name, Keys: t.DB.ExportAll()})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	sum := crc32.ChecksumIEEE(body)

	hdr := make([]byte, 4+4+8+4)
	copy(hdr[0:4], snapMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], snapVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(len(body)))
	binary.LittleEndian.PutUint32(hdr[16:20], sum)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if err := os.Rename(tmp, e.snapPath()); err != nil {
		e.abortCatchup()
		return err
	}
	e.lastSave.Store(time.Now().Unix())

	if e.cfg.Mode == ModeSnapshotAndAOF {
		// Snapshot is the new base; AOF becomes only the dual-written delta.
		return e.installCatchupAsAOF()
	}
	return nil
}

func (e *Engine) beginCatchup() {
	e.mu.Lock()
	e.catchingUp = true
	e.catchup.Reset()
	e.mu.Unlock()
}

func (e *Engine) abortCatchup() {
	e.mu.Lock()
	e.catchingUp = false
	e.catchup.Reset()
	e.mu.Unlock()
}

// installCatchupAsAOF replaces the live AOF with the catchup buffer contents only.
// Holds e.mu for the swap; writers block only for the rename window (same as any AOF write).
func (e *Engine) installCatchupAsAOF() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delta := append([]byte(nil), e.catchup.Bytes()...)
	e.catchingUp = false
	e.catchup.Reset()

	if e.aof != nil {
		_ = e.aof.Close()
		e.aof = nil
	}
	tmp := e.aofPath() + ".tmp"
	if err := os.WriteFile(tmp, delta, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, e.aofPath()); err != nil {
		return err
	}
	f, err := os.OpenFile(e.aofPath(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	e.aof = f
	if e.cfg.Fsync == FsyncAlways {
		return e.aof.Sync()
	}
	return nil
}

// rewriteAOF rebuilds appendonly.aof from the live dataset (AOF-only SAVE).
// Concurrent writes dual-write into catchup and are appended before the atomic rename.
// Caller (SaveSnapshot) must already hold the saving flag and have called beginCatchup.
func (e *Engine) rewriteAOF(reg *tenant.Registry) error {
	tmp := e.aofPath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		e.abortCatchup()
		return err
	}
	w := bufio.NewWriter(f)
	for _, t := range reg.All() {
		for _, r := range t.DB.ExportAll() {
			lines := recordToAOF(t.Name, r)
			for _, line := range lines {
				b, err := json.Marshal(line)
				if err != nil {
					f.Close()
					os.Remove(tmp)
					e.abortCatchup()
					return err
				}
				if _, err := w.Write(append(b, '\n')); err != nil {
					f.Close()
					os.Remove(tmp)
					e.abortCatchup()
					return err
				}
			}
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}

	// Serialize catchup drain + rename so no write is lost between export and cutover.
	e.mu.Lock()
	if _, err := f.Write(e.catchup.Bytes()); err != nil {
		e.mu.Unlock()
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if err := f.Sync(); err != nil {
		e.mu.Unlock()
		f.Close()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if err := f.Close(); err != nil {
		e.mu.Unlock()
		os.Remove(tmp)
		e.abortCatchup()
		return err
	}
	if e.aof != nil {
		_ = e.aof.Close()
		e.aof = nil
	}
	if err := os.Rename(tmp, e.aofPath()); err != nil {
		e.catchingUp = false
		e.catchup.Reset()
		e.mu.Unlock()
		return err
	}
	nf, err := os.OpenFile(e.aofPath(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		e.catchingUp = false
		e.catchup.Reset()
		e.mu.Unlock()
		return err
	}
	e.aof = nf
	e.catchingUp = false
	e.catchup.Reset()
	e.mu.Unlock()
	e.lastSave.Store(time.Now().Unix())
	return nil
}

// BGSave runs SaveSnapshot in a background goroutine.
func (e *Engine) BGSave(reg *tenant.Registry) error {
	if e.cfg.Mode == ModeNone {
		return fmt.Errorf("ERR Persistence disabled")
	}
	if e.saving.Load() {
		return fmt.Errorf("ERR Background save already in progress")
	}
	go func() { _ = e.SaveSnapshot(reg) }()
	return nil
}

// Load restores data into an already-constructed registry (empty DBs).
// Corruption policy: invalid snapshot CRC/magic/version → error (refuse load).
func (e *Engine) Load(reg *tenant.Registry) error {
	if e.cfg.Mode == ModeNone {
		return nil
	}
	switch e.cfg.Mode {
	case ModeSnapshot:
		err := e.loadSnapshot(reg)
		if err != nil && os.IsNotExist(err) {
			return nil // first boot: no dump yet
		}
		return err
	case ModeAOF:
		err := e.loadAOF(reg)
		if err != nil && os.IsNotExist(err) {
			return nil
		}
		return err
	case ModeSnapshotAndAOF:
		if err := e.loadSnapshot(reg); err != nil && !os.IsNotExist(err) {
			return err
		}
		// Always try AOF tail (may be empty after rewrite).
		if err := e.loadAOF(reg); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	default:
		return nil
	}
}

func (e *Engine) loadSnapshot(reg *tenant.Registry) error {
	f, err := os.Open(e.snapPath())
	if err != nil {
		return err
	}
	defer f.Close()
	hdr := make([]byte, 20)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return fmt.Errorf("persist: snapshot header: %w", err)
	}
	if string(hdr[0:4]) != snapMagic {
		return fmt.Errorf("persist: bad snapshot magic")
	}
	ver := binary.LittleEndian.Uint32(hdr[4:8])
	if ver != snapVersion {
		return fmt.Errorf("persist: unsupported snapshot version %d", ver)
	}
	bodyLen := binary.LittleEndian.Uint64(hdr[8:16])
	wantCRC := binary.LittleEndian.Uint32(hdr[16:20])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(f, body); err != nil {
		return fmt.Errorf("persist: snapshot body: %w", err)
	}
	if crc32.ChecksumIEEE(body) != wantCRC {
		return fmt.Errorf("persist: snapshot CRC mismatch (refusing load)")
	}
	var payload []snapTenant
	if err := json.Unmarshal(body, &payload); err != nil {
		return err
	}
	for _, st := range payload {
		t, ok := reg.Get(st.Name)
		if !ok {
			// skip unknown tenants (config removed) — document: skip tenant
			continue
		}
		if err := t.DB.LoadRecords(st.Keys); err != nil {
			return fmt.Errorf("persist: load tenant %s: %w", st.Name, err)
		}
	}
	info, _ := f.Stat()
	if info != nil {
		e.lastSave.Store(info.ModTime().Unix())
	}
	return nil
}

func (e *Engine) loadAOF(reg *tenant.Registry) error {
	f, err := os.Open(e.aofPath())
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	// large values
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec aofLine
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("persist: corrupt AOF line (refusing load): %w", err)
		}
		t, ok := reg.Get(rec.Tenant)
		if !ok {
			continue
		}
		if err := applyAOF(t.DB, rec); err != nil {
			return err
		}
	}
	return sc.Err()
}

func applyAOF(db *store.DB, rec aofLine) error {
	// Sinks are attached only after Load completes.
	switch rec.Op {
	case "SET":
		opt := store.SetOptions{}
		if rec.Exp > 0 {
			opt.HasExpireAt = true
			opt.ExpireAt = time.Unix(0, rec.Exp)
		}
		_, err := db.Set(rec.Key, rec.Value, opt)
		return err
	case "DEL":
		if len(rec.Keys) > 0 {
			db.Del(rec.Keys...)
		} else if rec.Key != "" {
			db.Del(rec.Key)
		}
		return nil
	case "EXPIRE":
		if rec.Exp > 0 {
			at := time.Unix(0, rec.Exp)
			d := time.Until(at)
			if d > 0 {
				db.Expire(rec.Key, d)
			} else {
				db.Del(rec.Key)
			}
		}
		return nil
	case "PERSIST":
		db.Persist(rec.Key)
		return nil
	case "FLUSHDB":
		db.FlushDB()
		return nil
	case "HSET", "HINCRBY":
		_, err := db.HSet(rec.Key, [][2]string{{rec.Field, rec.Value}})
		return err
	case "HDEL":
		_, err := db.HDel(rec.Key, rec.Args)
		return err
	case "LPUSH":
		_, err := db.LPush(rec.Key, rec.Args)
		return err
	case "RPUSH":
		_, err := db.RPush(rec.Key, rec.Args)
		return err
	case "LPOP", "RPOP":
		// full rewrite prefers RPUSH of remaining list; live log of pop is best-effort delete element
		// For load of incremental AOF after SET-style rewrite, pops are rare; apply by popping once.
		if rec.Op == "LPOP" {
			_, _, err := db.LPop(rec.Key)
			return err
		}
		_, _, err := db.RPop(rec.Key)
		return err
	case "SADD":
		_, err := db.SAdd(rec.Key, rec.Args)
		return err
	case "SREM":
		_, err := db.SRem(rec.Key, rec.Args)
		return err
	case "ZADD", "ZINCRBY":
		sc, err := strconv.ParseFloat(rec.Value, 64)
		if err != nil {
			return err
		}
		_, err = db.ZAdd(rec.Key, []store.ZMember{{Member: rec.Member, Score: sc}})
		return err
	case "ZREM":
		_, err := db.ZRem(rec.Key, rec.Args)
		return err
	default:
		return fmt.Errorf("persist: unknown AOF op %q", rec.Op)
	}
}

func recordToAOF(tenant string, r store.Record) []aofLine {
	exp := int64(0)
	if !r.ExpiresAt.IsZero() {
		exp = r.ExpiresAt.UnixNano()
	}
	typ := r.Type
	if typ == "" {
		typ = "string"
	}
	switch typ {
	case "hash":
		var lines []aofLine
		for f, v := range r.Hash {
			lines = append(lines, aofLine{Op: "HSET", Tenant: tenant, Key: r.Key, Field: f, Value: v, Exp: exp})
		}
		if len(lines) == 0 {
			return nil
		}
		// TTL only on first line is wrong for multi-field; apply exp after load via setExpireAt in LoadRecords.
		// For AOF rewrite, emit EXPIRE after fields.
		if exp > 0 {
			lines = append(lines, aofLine{Op: "EXPIRE", Tenant: tenant, Key: r.Key, Exp: exp})
		}
		return lines
	case "list":
		if len(r.List) == 0 {
			return nil
		}
		lines := []aofLine{{Op: "RPUSH", Tenant: tenant, Key: r.Key, Args: r.List}}
		if exp > 0 {
			lines = append(lines, aofLine{Op: "EXPIRE", Tenant: tenant, Key: r.Key, Exp: exp})
		}
		return lines
	case "set":
		if len(r.Set) == 0 {
			return nil
		}
		lines := []aofLine{{Op: "SADD", Tenant: tenant, Key: r.Key, Args: r.Set}}
		if exp > 0 {
			lines = append(lines, aofLine{Op: "EXPIRE", Tenant: tenant, Key: r.Key, Exp: exp})
		}
		return lines
	case "zset":
		var lines []aofLine
		for _, zm := range r.ZSet {
			lines = append(lines, aofLine{
				Op: "ZADD", Tenant: tenant, Key: r.Key, Member: zm.Member,
				Value: strconv.FormatFloat(zm.Score, 'f', -1, 64),
			})
		}
		if exp > 0 && len(lines) > 0 {
			lines = append(lines, aofLine{Op: "EXPIRE", Tenant: tenant, Key: r.Key, Exp: exp})
		}
		return lines
	default:
		line := aofLine{Op: "SET", Tenant: tenant, Key: r.Key, Value: r.Value, Exp: exp}
		return []aofLine{line}
	}
}

// AttachSinks wires AOF sinks to each tenant DB (call after Load).
func (e *Engine) AttachSinks(reg *tenant.Registry) {
	if e.cfg.Mode != ModeAOF && e.cfg.Mode != ModeSnapshotAndAOF {
		return
	}
	for _, t := range reg.All() {
		t.DB.SetMutationSink(&TenantSink{Engine: e, Tenant: t.Name})
	}
}

// StartPeriodicSnapshots runs SAVE on an interval when configured.
func (e *Engine) StartPeriodicSnapshots(reg *tenant.Registry) {
	if e.cfg.SnapshotInterval <= 0 {
		return
	}
	if e.cfg.Mode != ModeSnapshot && e.cfg.Mode != ModeSnapshotAndAOF {
		return
	}
	go func() {
		t := time.NewTicker(e.cfg.SnapshotInterval)
		defer t.Stop()
		for range t.C {
			_ = e.SaveSnapshot(reg)
		}
	}()
}
