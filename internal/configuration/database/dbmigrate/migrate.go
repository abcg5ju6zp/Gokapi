// Package dbmigrate implements a resumable, generation-isolated database
// migration. A migration pins a source snapshot and a unique migration
// identity, copies users (with end-to-end info), API keys, file metadata
// (with hotlinks) and file requests into an isolated staging generation in
// the destination, verifies the generation's integrity, marks it activatable
// and only then publishes it atomically. User sessions and statistics are not
// migrated.
package dbmigrate

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/forceu/gokapi/internal/configuration/database/dbabstraction"
	"github.com/forceu/gokapi/internal/configuration/database/provider/redis"
	"github.com/forceu/gokapi/internal/configuration/database/provider/sqlite"
	"github.com/forceu/gokapi/internal/models"
)

// probeMigrationState checks the destination for an existing migration state
// without creating or modifying it. Used to decide whether an interrupted
// migration can be resumed when the source is unavailable.
func probeMigrationState(config models.DbConnection) ([]byte, bool, error) {
	switch config.Type {
	case dbabstraction.TypeSqlite:
		return sqlite.MigrationStateExists(config)
	case dbabstraction.TypeRedis:
		return redis.MigrationStateExists(config)
	default:
		return nil, false, fmt.Errorf("unsupported database type %d", config.Type)
	}
}

// Run executes a resumable migration from the source to the destination
// connection. It is safe to call it repeatedly: an interrupted migration is
// resumed against the same pinned snapshot and generation, and an already
// completed migration is reported as success without changes.
func Run(sourceConfig, targetConfig models.DbConnection) error {
	if locationKey(sourceConfig) == locationKey(targetConfig) {
		return fmt.Errorf("source and destination must not point to the same database")
	}

	// Step 1: read the complete source snapshot before touching the target.
	// A source failure here cannot leave anything in the destination.
	sourceDb, sourceErr := connectSource(sourceConfig)
	var snap snapshot
	var snapshotAvailable bool
	if sourceErr == nil {
		snap, sourceErr = readSnapshot(sourceDb)
		sourceDb.Close()
		snapshotAvailable = sourceErr == nil
	}

	// If the source is unavailable, the destination is only opened when an
	// in-progress migration already exists there. The probe is strictly
	// read-only, so a fresh destination is never created in this path.
	if sourceErr != nil {
		_, found, probeErr := probeMigrationState(targetConfig)
		if probeErr != nil {
			return fmt.Errorf("source unavailable (%v); unable to probe destination for resumable state: %w", sourceErr, probeErr)
		}
		if !found {
			return fmt.Errorf("unable to read source database, no migration can be resumed: %w", sourceErr)
		}
	}

	// Step 2: open the destination and load an existing migration state.
	targetDb, err := dbabstraction.GetNew(targetConfig)
	if err != nil {
		return fmt.Errorf("unable to open destination database: %w", err)
	}
	defer targetDb.Close()
	backend, supported := targetDb.(StagingBackend)
	if !supported {
		return fmt.Errorf("destination database type does not support resumable migration")
	}
	if err := backend.MigPrepare(); err != nil {
		return fmt.Errorf("unable to prepare destination for migration: %w", err)
	}

	st, stateExists, err := loadState(backend)
	if err != nil {
		return err
	}

	if !stateExists {
		if sourceErr != nil {
			return fmt.Errorf("unable to read source database, no migration can be resumed: %w", sourceErr)
		}
		st, err = startNewMigration(backend, snap, sourceConfig, targetConfig)
		if err != nil {
			return err
		}
		fmt.Printf("Started new migration generation %s\n", st.MigrationId)
	} else {
		if err := validateExistingState(st, sourceConfig, targetConfig); err != nil {
			return err
		}
		switch st.Phase {
		case phaseActivated:
			if snapshotAvailable {
				currentChecksum, err := snap.checksum()
				if err != nil {
					return err
				}
				if currentChecksum != st.SnapshotChecksum {
					return fmt.Errorf("destination already contains a newer migration generation (%s); refusing to overwrite it", st.MigrationId)
				}
				fmt.Println("Migration already completed, nothing to do.")
			} else {
				fmt.Println("Migration generation already activated.")
			}
			// Staging may remain if the process died between activation and
			// cleanup.
			if err := backend.MigDropStaging(st.MigrationId); err != nil {
				fmt.Printf("Warning: unable to remove staging data of generation %s: %v\n", st.MigrationId, err)
			}
			return nil
		default:
			if !snapshotAvailable {
				if st.Phase == phaseCopying {
					return fmt.Errorf("source database unavailable and snapshot %s is incomplete: %w", st.MigrationId, sourceErr)
				}
				fmt.Printf("Source unavailable, resuming generation %s from pinned snapshot\n", st.MigrationId)
			} else {
				currentChecksum, err := snap.checksum()
				if err != nil {
					return err
				}
				if currentChecksum != st.SnapshotChecksum {
					return fmt.Errorf("source content changed since migration generation %s started; refusing to mix snapshots", st.MigrationId)
				}
				fmt.Printf("Resuming migration generation %s at phase %s\n", st.MigrationId, st.Phase)
			}
		}
	}

	// Step 3: stage all records of the pinned snapshot.
	if st.Phase == phaseCopying {
		if !snapshotAvailable {
			return fmt.Errorf("cannot continue copying generation %s without source access", st.MigrationId)
		}
		if err := copySnapshot(backend, st, snap); err != nil {
			return err
		}
		st.Phase = phaseCopied
		if err := saveState(backend, st, st.MigrationId); err != nil {
			return err
		}
		fmt.Println("All records staged.")
	}

	// Step 4: verify integrity. The resulting checksum is persisted
	// independently of the activatable marking.
	if st.Phase == phaseCopied {
		checksum, err := verifyStaging(backend, st.MigrationId, st.SourceDbVersion, st.Manifest)
		if err != nil {
			return err
		}
		st.StagedChecksum = checksum
		st.Phase = phaseVerified
		if err := saveState(backend, st, st.MigrationId); err != nil {
			return err
		}
		fmt.Println("Integrity verification passed.")
	}

	// Step 5: mark the generation as safe to enable. This is a separate
	// persisted step, so a failure here never requires recopying data.
	if st.Phase == phaseVerified {
		st.Phase = phaseActivatable
		if err := saveState(backend, st, st.MigrationId); err != nil {
			return err
		}
		fmt.Println("Generation marked as activatable.")
	}

	// Step 6: publish the generation atomically.
	if st.Phase == phaseActivatable {
		if err := backend.MigActivate(st.MigrationId, targetDb.GetSchemaVersion(), st.Manifest.ApiKeyExpiries); err != nil {
			return fmt.Errorf("activating migration generation failed, previous target data kept: %w", err)
		}
		st.Phase = phaseActivated
		if err := saveState(backend, st, st.MigrationId); err != nil {
			return fmt.Errorf("generation activated but recording final state failed: %w", err)
		}
		fmt.Println("Migration generation activated.")
		if err := backend.MigDropStaging(st.MigrationId); err != nil {
			fmt.Printf("Warning: unable to remove staging data of generation %s: %v\n", st.MigrationId, err)
		}
	}

	fmt.Println("Migration completed successfully.")
	return nil
}

func connectSource(config models.DbConnection) (dbabstraction.Database, error) {
	var result dbabstraction.Database
	var connectErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				connectErr = fmt.Errorf("source database connection failed: %v", recovered)
			}
		}()
		result, connectErr = dbabstraction.GetNew(config)
	}()
	return result, connectErr
}

func startNewMigration(backend StagingBackend, snap snapshot, sourceConfig, targetConfig models.DbConnection) (*state, error) {
	checksum, err := snap.checksum()
	if err != nil {
		return nil, err
	}
	st, err := newState(snap, sourceConfig.Type, targetConfig.Type, locationKey(sourceConfig), locationKey(targetConfig), checksum)
	if err != nil {
		return nil, err
	}
	blob, err := st.json()
	if err != nil {
		return nil, err
	}
	created, err := backend.MigCreateState(st.MigrationId, blob)
	if err != nil {
		return nil, fmt.Errorf("unable to record migration state: %w", err)
	}
	if !created {
		// Another process started a migration concurrently: adopt its state.
		existing, exists, err := backend.MigLoadState()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("migration state appeared and disappeared concurrently")
		}
		loaded, err := parseState(existing)
		if err != nil {
			return nil, err
		}
		return &loaded, nil
	}
	return st, nil
}

func validateExistingState(st *state, sourceConfig, targetConfig models.DbConnection) error {
	if st.SourceType != sourceConfig.Type || st.SourceLocation != locationKey(sourceConfig) {
		return fmt.Errorf("destination contains an in-progress migration from a different source (%s type %d)",
			st.SourceLocation, st.SourceType)
	}
	if st.TargetType != targetConfig.Type || st.TargetLocation != locationKey(targetConfig) {
		return fmt.Errorf("migration state was recorded for a different destination (%s type %d)",
			st.TargetLocation, st.TargetType)
	}
	return nil
}

func loadState(backend StagingBackend) (*state, bool, error) {
	blob, exists, err := backend.MigLoadState()
	if err != nil {
		return nil, false, fmt.Errorf("unable to load migration state: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	st, err := parseState(blob)
	if err != nil {
		return nil, false, err
	}
	return &st, true, nil
}

func saveState(backend StagingBackend, st *state, expectedId string) error {
	blob, err := st.json()
	if err != nil {
		return err
	}
	if err := backend.MigSaveStateCAS(expectedId, blob); err != nil {
		return fmt.Errorf("unable to update migration state: %w", err)
	}
	return nil
}

// copySnapshot stages every record of the snapshot. Existing, confirmed
// records are skipped, so an interrupted copy only fills missing records.
// When a write result is uncertain, the staged record is read back and
// compared before continuing.
func copySnapshot(backend StagingBackend, st *state, snap snapshot) error {
	gen := st.MigrationId

	userMap := make(map[int]models.User, len(snap.users))
	userIds := make([]int, 0, len(snap.users))
	for _, user := range snap.users {
		userMap[user.Id] = user
		userIds = append(userIds, user.Id)
	}
	sort.Ints(userIds)
	for _, id := range userIds {
		user := userMap[id]
		if err := ensureRecord(backend, gen, kindUser, strconv.Itoa(id), user,
			backend.MigStageUser,
			func(g string, idString string) (models.User, bool, error) {
				userId, _ := strconv.Atoi(idString)
				return backend.MigGetStagedUser(g, userId)
			}); err != nil {
			return err
		}
	}

	e2eIds := make([]int, 0, len(snap.e2e))
	for userId := range snap.e2e {
		e2eIds = append(e2eIds, userId)
	}
	sort.Ints(e2eIds)
	for _, userId := range e2eIds {
		info := snap.e2e[userId]
		if err := ensureRecord(backend, gen, kindE2E, strconv.Itoa(userId), info,
			func(g string, value models.E2EInfoEncrypted) error {
				return backend.MigStageEnd2End(g, userId, value)
			},
			func(g string, idString string) (models.E2EInfoEncrypted, bool, error) {
				uid, _ := strconv.Atoi(idString)
				return backend.MigGetStagedEnd2End(g, uid)
			}); err != nil {
			return err
		}
	}

	for _, id := range sortedKeys(snap.apiKeys) {
		apiKey := snap.apiKeys[id]
		if err := ensureRecord(backend, gen, kindApiKey, id, apiKey,
			backend.MigStageApiKey, backend.MigGetStagedApiKey); err != nil {
			return err
		}
	}

	requestMap := make(map[string]models.FileRequest, len(snap.requests))
	requestIds := make([]string, 0, len(snap.requests))
	for _, request := range snap.requests {
		requestMap[request.Id] = request
		requestIds = append(requestIds, request.Id)
	}
	sort.Strings(requestIds)
	for _, id := range requestIds {
		request := requestMap[id]
		if err := ensureRecord(backend, gen, kindFileRequest, id, request,
			backend.MigStageFileRequest, backend.MigGetStagedFileRequest); err != nil {
			return err
		}
	}

	for _, id := range sortedKeys(snap.files) {
		file := snap.files[id]
		if err := ensureRecord(backend, gen, kindFile, id, file,
			backend.MigStageFile, backend.MigGetStagedFile); err != nil {
			return err
		}
	}

	for _, id := range sortedKeys(snap.hotlinks) {
		fileId := snap.hotlinks[id]
		if err := ensureRecord(backend, gen, kindHotlink, id, fileId,
			func(g string, value string) error {
				return backend.MigStageHotlink(g, id, value)
			},
			backend.MigGetStagedHotlink); err != nil {
			return err
		}
	}

	if err := backend.MigStageUserIdMax(gen, snap.userIdMax); err != nil {
		// Uncertain result: verify whether the counter landed.
		actual, ok, readErr := backend.MigGetStagedUserIdMax(gen)
		if readErr != nil || !ok || actual != snap.userIdMax {
			return fmt.Errorf("unable to stage user id counter: %w", err)
		}
	}
	return nil
}

// ensureRecord stages a single record. If an identical record is already
// present, it is left untouched. On an uncertain write response the record is
// read back and only accepted when it matches the snapshot content.
func ensureRecord[T any](backend StagingBackend, gen, kind, id string, wanted T,
	write func(string, T) error,
	read func(string, string) (T, bool, error)) error {

	existing, exists, err := read(gen, id)
	if err == nil && exists {
		equal, err := recordsEqual(kind, existing, wanted)
		if err != nil {
			return err
		}
		if equal {
			return nil
		}
		return fmt.Errorf("staged %s %s contains conflicting content; aborting to prevent mixing", kind, id)
	}

	writeErr := write(gen, wanted)
	if writeErr != nil {
		actual, exists, readErr := read(gen, id)
		if readErr == nil && exists {
			equal, equalErr := recordsEqual(kind, actual, wanted)
			if equalErr == nil && equal {
				// Write response was uncertain but the record did land.
				return nil
			}
		}
		return fmt.Errorf("writing %s %s failed: %w", kind, id, writeErr)
	}

	actual, exists, err := read(gen, id)
	if err != nil {
		return fmt.Errorf("unable to confirm staged %s %s: %w", kind, id, err)
	}
	if !exists {
		return fmt.Errorf("staged %s %s could not be found after writing", kind, id)
	}
	equal, err := recordsEqual(kind, actual, wanted)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("staged %s %s does not match source content", kind, id)
	}
	return nil
}

func recordsEqual(kind string, a, b any) (bool, error) {
	hashA, err := recordHash(kind, a)
	if err != nil {
		return false, err
	}
	hashB, err := recordHash(kind, b)
	if err != nil {
		return false, err
	}
	return hashA == hashB, nil
}

// locationKey identifies a concrete database namespace. Two connections with
// the same key address the same data.
func locationKey(config models.DbConnection) string {
	return strconv.Itoa(config.Type) + "://" + config.HostUrl + "#" + config.RedisPrefix
}
