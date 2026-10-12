package dbmigrate

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Migration phases. Every transition is persisted separately, so that a crash
// at any point only resumes from the last completed phase.
const (
	// phaseCopying: snapshot pinned and stored, records are being staged.
	phaseCopying = "copying"
	// phaseCopied: every snapshot record has been staged and confirmed.
	phaseCopied = "copied"
	// phaseVerified: integrity verification of the staged generation passed
	// and the checksum was recorded (separately from the activatable mark).
	phaseVerified = "verified"
	// phaseActivatable: generation was explicitly marked safe to enable.
	phaseActivatable = "activatable"
	// phaseActivated: generation was published as live data.
	phaseActivated = "activated"
)

// manifest is the immutable object list of the pinned source snapshot.
// It contains the per-record content hash and allows integrity verification
// and resume without any access to the original source.
type manifest struct {
	// Counts contains the number of records per kind.
	Counts map[string]int `json:"counts"`
	// Hashes maps "kind:objectId" to the content hash of the record.
	Hashes map[string]string `json:"hashes"`
	// UserIdMax is the highest user id, required by the Redis id counter.
	UserIdMax int `json:"userIdMax"`
	// ApiKeyExpiries contains expiry timestamps for API keys that expire.
	ApiKeyExpiries map[string]int64 `json:"apiKeyExpiries"`
}

// state is the persisted migration state stored inside the destination.
type state struct {
	// MigrationId identifies the isolated destination generation.
	MigrationId string `json:"migrationId"`

	SourceType     int    `json:"sourceType"`
	SourceLocation string `json:"sourceLocation"`
	TargetType     int    `json:"targetType"`
	TargetLocation string `json:"targetLocation"`

	// SourceDbVersion pins the database version that the snapshot was taken
	// from, as required by the fixed source-version guarantee.
	SourceDbVersion int `json:"sourceDbVersion"`

	StartedAt int64  `json:"startedAt"`
	Phase     string `json:"phase"`

	// SnapshotChecksum is the fingerprint of the complete source snapshot
	// captured when the migration started.
	SnapshotChecksum string `json:"snapshotChecksum"`
	// StagedChecksum is recorded only after integrity verification passed.
	// It is persisted independently of the activatable mark.
	StagedChecksum string `json:"stagedChecksum"`

	Manifest manifest `json:"manifest"`
}

func newMigrationId() (string, error) {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func newState(s snapshot, sourceType, targetType int, sourceLocation, targetLocation, checksum string) (*state, error) {
	id, err := newMigrationId()
	if err != nil {
		return nil, err
	}
	return &state{
		MigrationId:      id,
		SourceType:       sourceType,
		SourceLocation:   sourceLocation,
		TargetType:       targetType,
		TargetLocation:   targetLocation,
		SourceDbVersion:  s.version,
		StartedAt:        time.Now().Unix(),
		Phase:            phaseCopying,
		SnapshotChecksum: checksum,
		Manifest:         s.toManifest(),
	}, nil
}

func (s state) json() ([]byte, error) {
	blob, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("unable to encode migration state: %w", err)
	}
	return blob, nil
}

func parseState(blob []byte) (state, error) {
	var result state
	err := json.Unmarshal(blob, &result)
	if err != nil {
		return state{}, fmt.Errorf("unable to parse migration state: %w", err)
	}
	return result, nil
}
