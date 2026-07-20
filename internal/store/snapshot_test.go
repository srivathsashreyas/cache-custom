package store

import (
	"testing"
	"time"
)

func TestExportLoadRecords(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: StrategyGlobalTrack, Policy: PolicyAllKeysLRU})
	defer db.Close()
	_, _ = db.Set("a", "1", SetOptions{})
	_, _ = db.Set("b", "2", SetOptions{EX: time.Hour})
	recs := db.ExportAll()
	if len(recs) != 2 {
		t.Fatalf("%d", len(recs))
	}
	db2 := New(Config{MaxMemory: 1 << 20, ShardCount: 2, Strategy: StrategyGlobalTrack, Policy: PolicyAllKeysLRU})
	defer db2.Close()
	if err := db2.LoadRecords(recs); err != nil {
		t.Fatal(err)
	}
	if v, ok := db2.Get("a"); !ok || v != "1" {
		t.Fatal(v, ok)
	}
	if v, ok := db2.Get("b"); !ok || v != "2" {
		t.Fatal(v, ok)
	}
}

func TestFlushDB(t *testing.T) {
	db := New(Config{MaxMemory: 1 << 20, ShardCount: 1, Strategy: StrategyGlobalTrack})
	defer db.Close()
	_, _ = db.Set("a", "1", SetOptions{})
	if db.FlushDB() < 1 {
		t.Fatal()
	}
	if db.DBSize() != 0 {
		t.Fatal(db.DBSize())
	}
}
