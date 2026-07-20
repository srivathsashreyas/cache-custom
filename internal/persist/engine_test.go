package persist

import (
	"fmt"
	"os"
	"sync"
	"path/filepath"
	"testing"
	"time"

	"cache-custom/internal/store"
	"cache-custom/internal/tenant"
)

func testRegistry(t *testing.T) *tenant.Registry {
	t.Helper()
	r, err := tenant.NewRegistry([]tenant.Config{
		{Name: "App1", Password: "p1", MaxMemory: 1 << 20, Strategy: store.StrategyGlobalTrack, Policy: store.PolicyAllKeysLRU, ShardCount: 2},
		{Name: "App2", Password: "p2", MaxMemory: 1 << 20, Strategy: store.StrategySteal, Policy: store.PolicyAllKeysFIFO, ShardCount: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	return r
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	a, _ := reg.Get("App1")
	b, _ := reg.Get("App2")
	_, _ = a.DB.Set("k1", "v1", store.SetOptions{})
	_, _ = a.DB.Set("k2", "v2", store.SetOptions{EX: time.Hour})
	_, _ = b.DB.Set("only2", "x", store.SetOptions{})

	eng := New(Config{Mode: ModeSnapshot, Dir: dir})
	if err := eng.Open(); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveSnapshot(reg); err != nil {
		t.Fatal(err)
	}
	eng.Close()

	// New empty registry, load snapshot
	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeSnapshot, Dir: dir})
	if err := eng2.Open(); err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()
	if err := eng2.Load(reg2); err != nil {
		t.Fatal(err)
	}
	a2, _ := reg2.Get("App1")
	b2, _ := reg2.Get("App2")
	if v, ok := a2.DB.Get("k1"); !ok || v != "v1" {
		t.Fatalf("k1 %v %v", v, ok)
	}
	if v, ok := a2.DB.Get("k2"); !ok || v != "v2" {
		t.Fatalf("k2 %v %v", v, ok)
	}
	if v, ok := b2.DB.Get("only2"); !ok || v != "x" {
		t.Fatalf("only2 %v %v", v, ok)
	}
	// isolation: App2 must not see App1 keys
	if _, ok := b2.DB.Get("k1"); ok {
		t.Fatal("cross-tenant leak")
	}
	if eng2.LastSaveUnix() == 0 {
		t.Fatal("lastsave")
	}
}

func TestSnapshotCRCRefuse(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("k", "v", store.SetOptions{})
	eng := New(Config{Mode: ModeSnapshot, Dir: dir})
	_ = eng.Open()
	_ = eng.SaveSnapshot(reg)
	eng.Close()

	// Corrupt body
	path := filepath.Join(dir, snapFile)
	b, _ := os.ReadFile(path)
	if len(b) > 25 {
		b[25] ^= 0xff
	}
	_ = os.WriteFile(path, b, 0o644)

	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeSnapshot, Dir: dir})
	_ = eng2.Open()
	defer eng2.Close()
	if err := eng2.Load(reg2); err == nil {
		t.Fatal("expected CRC failure")
	}
}

func TestAOFRoundTrip(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	eng := New(Config{Mode: ModeAOF, Dir: dir, Fsync: FsyncAlways})
	if err := eng.Open(); err != nil {
		t.Fatal(err)
	}
	eng.AttachSinks(reg)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("a", "1", store.SetOptions{})
	_, _ = a.DB.Set("b", "2", store.SetOptions{EX: time.Hour})
	a.DB.Del("a")
	eng.Close()

	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeAOF, Dir: dir, Fsync: FsyncAlways})
	_ = eng2.Open()
	defer eng2.Close()
	if err := eng2.Load(reg2); err != nil {
		t.Fatal(err)
	}
	a2, _ := reg2.Get("App1")
	if _, ok := a2.DB.Get("a"); ok {
		t.Fatal("a should be deleted via AOF")
	}
	if v, ok := a2.DB.Get("b"); !ok || v != "2" {
		t.Fatalf("b %v %v", v, ok)
	}
}

func TestHybridSnapshotThenAOF(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	eng := New(Config{Mode: ModeSnapshotAndAOF, Dir: dir, Fsync: FsyncAlways})
	_ = eng.Open()
	eng.AttachSinks(reg)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("base", "snap", store.SetOptions{})
	if err := eng.SaveSnapshot(reg); err != nil {
		t.Fatal(err)
	}
	// After SAVE, AOF truncated; new writes append.
	_, _ = a.DB.Set("tail", "aof", store.SetOptions{})
	eng.Close()

	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeSnapshotAndAOF, Dir: dir, Fsync: FsyncAlways})
	_ = eng2.Open()
	defer eng2.Close()
	if err := eng2.Load(reg2); err != nil {
		t.Fatal(err)
	}
	a2, _ := reg2.Get("App1")
	if v, ok := a2.DB.Get("base"); !ok || v != "snap" {
		t.Fatalf("base %v %v", v, ok)
	}
	if v, ok := a2.DB.Get("tail"); !ok || v != "aof" {
		t.Fatalf("tail %v %v", v, ok)
	}
}

func TestModeNoneNoFiles(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	eng := New(Config{Mode: ModeNone, Dir: dir})
	_ = eng.Open()
	eng.AttachSinks(reg)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("k", "v", store.SetOptions{})
	_ = eng.SaveSnapshot(reg)
	eng.Close()
	// no dump required
	if _, err := os.Stat(filepath.Join(dir, snapFile)); err == nil {
		t.Fatal("snapshot should not be written in mode none")
	}
}

func TestAOFRewritePreservesConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	eng := New(Config{Mode: ModeAOF, Dir: dir, Fsync: FsyncNo})
	if err := eng.Open(); err != nil {
		t.Fatal(err)
	}
	eng.AttachSinks(reg)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("base", "0", store.SetOptions{})

	// Hammer writes while rewrite runs.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = a.DB.Set(fmt.Sprintf("c%d", i%50), fmt.Sprintf("v%d", i), store.SetOptions{})
				i++
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	if err := eng.SaveSnapshot(reg); err != nil {
		close(stop)
		wg.Wait()
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	// One more write after rewrite must land on live AOF.
	_, _ = a.DB.Set("after", "1", store.SetOptions{})
	eng.Close()

	// Reload: must see base, after, and not crash.
	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeAOF, Dir: dir, Fsync: FsyncNo})
	_ = eng2.Open()
	defer eng2.Close()
	if err := eng2.Load(reg2); err != nil {
		t.Fatal(err)
	}
	a2, _ := reg2.Get("App1")
	if v, ok := a2.DB.Get("after"); !ok || v != "1" {
		t.Fatalf("after rewrite write lost: %v %v", v, ok)
	}
	if v, ok := a2.DB.Get("base"); !ok || v != "0" {
		t.Fatalf("base lost: %v %v", v, ok)
	}
}

func TestHybridSavePreservesConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	reg := testRegistry(t)
	eng := New(Config{Mode: ModeSnapshotAndAOF, Dir: dir, Fsync: FsyncAlways})
	_ = eng.Open()
	eng.AttachSinks(reg)
	a, _ := reg.Get("App1")
	_, _ = a.DB.Set("base", "snap", store.SetOptions{})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = a.DB.Set("hot", fmt.Sprintf("v%d", i), store.SetOptions{})
				i++
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	if err := eng.SaveSnapshot(reg); err != nil {
		close(stop)
		wg.Wait()
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	_, _ = a.DB.Set("post", "yes", store.SetOptions{})
	eng.Close()

	reg2 := testRegistry(t)
	eng2 := New(Config{Mode: ModeSnapshotAndAOF, Dir: dir, Fsync: FsyncAlways})
	_ = eng2.Open()
	defer eng2.Close()
	if err := eng2.Load(reg2); err != nil {
		t.Fatal(err)
	}
	a2, _ := reg2.Get("App1")
	if v, ok := a2.DB.Get("base"); !ok || v != "snap" {
		t.Fatalf("base %v %v", v, ok)
	}
	if v, ok := a2.DB.Get("post"); !ok || v != "yes" {
		t.Fatalf("post-save write lost: %v %v", v, ok)
	}
	// hot should exist with some value from concurrent SETs
	if _, ok := a2.DB.Get("hot"); !ok {
		t.Fatal("concurrent hybrid writes to hot lost")
	}
}
