package persist

import "strings"

// Mode is the durability mode (D007).
type Mode string

const (
	ModeNone          Mode = "none"
	ModeSnapshot      Mode = "snapshot"
	ModeAOF           Mode = "aof"
	ModeSnapshotAndAOF Mode = "snapshot+aof"
)

// ParseMode maps config strings to Mode.
func ParseMode(s string) (Mode, bool) {
	m := Mode(strings.ToLower(strings.TrimSpace(s)))
	switch m {
	case ModeNone, ModeSnapshot, ModeAOF, ModeSnapshotAndAOF, "":
		if m == "" {
			return ModeNone, true
		}
		return m, true
	default:
		return "", false
	}
}

// FsyncPolicy controls AOF durability.
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

func ParseFsync(s string) FsyncPolicy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "always":
		return FsyncAlways
	case "no":
		return FsyncNo
	default:
		return FsyncEverySec
	}
}
