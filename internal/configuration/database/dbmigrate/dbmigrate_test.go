package dbmigrate

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/forceu/gokapi/internal/configuration/database/dbabstraction"
	"github.com/forceu/gokapi/internal/models"
)

const redisAddr = "127.0.0.1:16410"

var miniRedis *miniredis.Miniredis

func TestMain(m *testing.M) {
	miniRedis = miniredis.NewMiniRedis()
	if err := miniRedis.StartAddr(redisAddr); err != nil {
		log.Fatal("Could not start miniredis: ", err)
	}
	exitCode := m.Run()
	miniRedis.Close()
	os.Exit(exitCode)
}

func sqliteConfig(t *testing.T, name string) models.DbConnection {
	return models.DbConnection{
		Type:    dbabstraction.TypeSqlite,
		HostUrl: filepath.Join(t.TempDir(), name+".sqlite"),
	}
}

var redisPrefixCounter int64

func redisConfig(prefix string) models.DbConnection {
	// A unique prefix per call keeps repeated test runs (go test -count=N)
	// independent, since a single miniredis instance is shared.
	sequence := atomic.AddInt64(&redisPrefixCounter, 1)
	return models.DbConnection{
		Type:        dbabstraction.TypeRedis,
		HostUrl:     redisAddr,
		RedisPrefix: fmt.Sprintf("%s%d_", prefix, sequence),
	}
}

func openDB(t *testing.T, config models.DbConnection) dbabstraction.Database {
	t.Helper()
	db, err := dbabstraction.GetNew(config)
	if err != nil {
		t.Fatalf("connecting %v: %v", config, err)
	}
	return db
}

// seedData fills a database with users, end-to-end info, API keys, files,
// hotlinks and file requests, including all association types. It also
// creates a session and traffic statistics, which are not migrated.
func seedData(t *testing.T, db dbabstraction.Database) {
	t.Helper()
	admin := models.User{
		Id:          5,
		Name:        "alice",
		Password:    "secretpw",
		Permissions: models.UserPermissionAll,
		UserLevel:   models.UserLevelSuperAdmin,
	}
	user := models.User{
		Id:            7,
		Name:          "bob",
		Password:      "userpw",
		Permissions:   models.UserPermGuestUploads,
		UserLevel:     models.UserLevelUser,
		LastOnline:    123456,
		ResetPassword: true,
	}
	db.SaveUser(admin, false)
	db.SaveUser(user, false)
	db.SaveEnd2EndInfo(models.E2EInfoEncrypted{
		Version: 5,
		Nonce:   []byte("nonce-1234"),
		Content: []byte("encrypted-e2e-content"),
	}, 5)

	request := models.FileRequest{
		Id:           "request-1",
		Name:         "Uploads",
		UserId:       5,
		MaxFiles:     3,
		MaxSize:      100,
		Expiry:       0,
		CreationDate: 999,
		ApiKey:       "key-request",
		Notes:        "please upload",
	}
	db.SaveFileRequest(request)
	db.SaveApiKey(models.ApiKey{
		Id:           "key-admin",
		PublicId:     "public-admin",
		FriendlyName: "Admin key",
		LastUsed:     42,
		Permissions:  models.ApiPermView | models.ApiPermUpload,
		Expiry:       4102444800,
		UserId:       5,
	})
	db.SaveApiKey(models.ApiKey{
		Id:              "key-request",
		PublicId:        "public-request",
		FriendlyName:    "Request key",
		Permissions:     models.ApiPermUpload,
		UserId:          5,
		UploadRequestId: "request-1",
	})

	db.SaveMetaData(models.File{
		Id:                 "file-hotlink",
		Name:               "picture.png",
		Size:               "4 B",
		SHA1:               "sha1pic",
		ExpireAt:           2147483646,
		SizeBytes:          4,
		ContentType:        "image/png",
		HotlinkId:          "hotlink-1",
		UserId:             5,
		UploadDate:         100,
		DownloadsRemaining: 9,
		Encryption: models.EncryptionInfo{
			IsEncrypted:   true,
			Nonce:         []byte("file-nonce"),
			DecryptionKey: []byte("file-key"),
		},
	})
	db.SaveHotlink(models.File{Id: "file-hotlink", HotlinkId: "hotlink-1"})
	db.SaveMetaData(models.File{
		Id:                 "file-request",
		Name:               "upload.bin",
		Size:               "2 B",
		SHA1:               "sha1req",
		SizeBytes:          2,
		ContentType:        "application/octet-stream",
		UserId:             5,
		UploadDate:         101,
		UnlimitedTime:      true,
		UnlimitedDownloads: true,
		UploadRequestId:    "request-1",
	})

	db.SaveSession("source-session", models.Session{
		RenewAt:    2147483645,
		ValidUntil: 2147483646,
		UserId:     5,
	})
	db.SaveStatTraffic(7777)
}

func seedStaleTargetData(t *testing.T, db dbabstraction.Database, sessionId string) {
	t.Helper()
	db.SaveUser(models.User{Id: 99, Name: "staleuser", UserLevel: models.UserLevelUser}, false)
	db.SaveApiKey(models.ApiKey{Id: "stale-key", PublicId: "stale-pub", UserId: 99})
	db.SaveFileRequest(models.FileRequest{Id: "stale-request", UserId: 99})
	db.SaveMetaData(models.File{Id: "stale-file", Name: "old", UserId: 99})
	db.SaveHotlink(models.File{Id: "stale-file", HotlinkId: "stale-hotlink"})
	db.SaveSession(sessionId, models.Session{ValidUntil: 2147483646, UserId: 99})
	db.SaveStatTraffic(333)
}

func assertContentEqual(t *testing.T, source, target dbabstraction.Database, prefix string) {
	t.Helper()

	sourceUsers := source.GetAllUsers()
	targetUsers := target.GetAllUsers()
	if len(sourceUsers) != len(targetUsers) {
		t.Fatalf("%s: user count %d != %d", prefix, len(sourceUsers), len(targetUsers))
	}
	targetUserMap := map[int]models.User{}
	for _, user := range targetUsers {
		targetUserMap[user.Id] = user
	}
	for _, user := range sourceUsers {
		actual, ok := targetUserMap[user.Id]
		if !ok {
			t.Fatalf("%s: user %d missing", prefix, user.Id)
		}
		if !reflect.DeepEqual(user, actual) {
			t.Fatalf("%s: user %d differs: %+v != %+v", prefix, user.Id, user, actual)
		}
		e2eSource := source.GetEnd2EndInfo(user.Id)
		e2eTarget := target.GetEnd2EndInfo(user.Id)
		if !reflect.DeepEqual(e2eSource, e2eTarget) {
			t.Fatalf("%s: e2e info of user %d differs", prefix, user.Id)
		}
	}

	sourceKeys := source.GetAllApiKeys()
	targetKeys := target.GetAllApiKeys()
	if len(sourceKeys) != len(targetKeys) {
		t.Fatalf("%s: api key count %d != %d", prefix, len(sourceKeys), len(targetKeys))
	}
	for id, expected := range sourceKeys {
		actual, ok := targetKeys[id]
		if !ok {
			t.Fatalf("%s: api key %s missing", prefix, id)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("%s: api key %s differs: %+v != %+v", prefix, id, expected, actual)
		}
	}

	sourceFiles := source.GetAllMetadata()
	targetFiles := target.GetAllMetadata()
	if len(sourceFiles) != len(targetFiles) {
		t.Fatalf("%s: file count %d != %d", prefix, len(sourceFiles), len(targetFiles))
	}
	for id, expected := range sourceFiles {
		actual, ok := targetFiles[id]
		if !ok {
			t.Fatalf("%s: file %s missing", prefix, id)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("%s: file %s differs: %+v != %+v", prefix, id, expected, actual)
		}
		sourceFileId, sourceOk := source.GetHotlink(expected.HotlinkId)
		targetFileId, targetOk := target.GetHotlink(expected.HotlinkId)
		if sourceOk != targetOk || (sourceOk && sourceFileId != targetFileId) {
			t.Fatalf("%s: hotlink %s not preserved", prefix, expected.HotlinkId)
		}
	}

	sourceRequests := source.GetAllFileRequests()
	targetRequests := target.GetAllFileRequests()
	if len(sourceRequests) != len(targetRequests) {
		t.Fatalf("%s: request count %d != %d", prefix, len(sourceRequests), len(targetRequests))
	}
	targetRequestMap := map[string]models.FileRequest{}
	for _, request := range targetRequests {
		targetRequestMap[request.Id] = request
	}
	for _, expected := range sourceRequests {
		actual, ok := targetRequestMap[expected.Id]
		if !ok {
			t.Fatalf("%s: request %s missing", prefix, expected.Id)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("%s: request %s differs: %+v != %+v", prefix, expected.Id, expected, actual)
		}
	}
}

func assertSessionAbsent(t *testing.T, db dbabstraction.Database, id, prefix string) {
	t.Helper()
	if _, ok := db.GetSession(id); ok {
		t.Fatalf("%s: session %s was migrated despite convention", prefix, id)
	}
}

func assertSessionPresent(t *testing.T, db dbabstraction.Database, id, prefix string) {
	t.Helper()
	if _, ok := db.GetSession(id); !ok {
		t.Fatalf("%s: pre-existing session %s was removed", prefix, id)
	}
}

func runMigration(t *testing.T, source, target models.DbConnection) {
	t.Helper()
	if err := Run(source, target); err != nil {
		t.Fatalf("migration failed: %v", err)
	}
}

func TestSqliteToSqlite(t *testing.T) {
	source := sqliteConfig(t, "source")
	target := sqliteConfig(t, "target")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	runMigration(t, source, target)

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb := openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "sqlite->sqlite")
	assertSessionAbsent(t, targetDb, "source-session", "sqlite->sqlite")
}

func TestSqliteToRedis(t *testing.T) {
	source := sqliteConfig(t, "source")
	target := redisConfig("ut_s2r_")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	runMigration(t, source, target)

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb := openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "sqlite->redis")
	assertSessionAbsent(t, targetDb, "source-session", "sqlite->redis")
}

func TestRedisToSqlite(t *testing.T) {
	source := redisConfig("ut_r2s_src_")
	target := sqliteConfig(t, "target")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	runMigration(t, source, target)

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb := openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "redis->sqlite")
	assertSessionAbsent(t, targetDb, "source-session", "redis->sqlite")
}

func TestRedisToRedisReplacesStaleDataAndKeepsSessions(t *testing.T) {
	source := redisConfig("ut_r2r_src_")
	target := redisConfig("ut_r2r_dst_")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	targetDb := openDB(t, target)
	seedStaleTargetData(t, targetDb, "target-session")
	targetDb.Close()

	runMigration(t, source, target)

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb = openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "redis->redis")

	// No stale generation content may remain.
	if _, ok := targetDb.GetUser(99); ok {
		t.Fatal("stale user from old generation was not removed")
	}
	if _, ok := targetDb.GetApiKey("stale-key"); ok {
		t.Fatal("stale api key was not removed")
	}
	if _, ok := targetDb.GetMetaDataById("stale-file"); ok {
		t.Fatal("stale file was not removed")
	}
	if _, ok := targetDb.GetHotlink("stale-hotlink"); ok {
		t.Fatal("stale hotlink was not removed")
	}
	if _, ok := targetDb.GetFileRequest("stale-request"); ok {
		t.Fatal("stale file request was not removed")
	}
	// Source sessions are not migrated, destination sessions are preserved.
	assertSessionAbsent(t, targetDb, "source-session", "redis->redis")
	assertSessionPresent(t, targetDb, "target-session", "redis->redis")
	// Statistics are preserved as well.
	if traffic := targetDb.GetStatTraffic(); traffic != 333 {
		t.Fatalf("destination statistics were modified: %d", traffic)
	}
}

func TestEmptySourceAllDirections(t *testing.T) {
	cases := []struct {
		name   string
		source models.DbConnection
		target models.DbConnection
	}{
		{"empty sqlite to sqlite", sqliteConfig(t, "empty-src-1"), sqliteConfig(t, "empty-dst-1")},
		{"empty sqlite to redis", sqliteConfig(t, "empty-src-2"), redisConfig("ut_empty_s2r_")},
		{"empty redis to sqlite", redisConfig("ut_empty_r2s_"), sqliteConfig(t, "empty-dst-3")},
		{"empty redis to redis", redisConfig("ut_empty_r2r_src_"), redisConfig("ut_empty_r2r_dst_")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDb := openDB(t, tc.source)
			if len(sourceDb.GetAllUsers()) != 0 {
				t.Fatal("source expected to be empty")
			}
			sourceDb.Close()

			if err := Run(tc.source, tc.target); err != nil {
				t.Fatalf("empty migration failed: %v", err)
			}

			targetDb := openDB(t, tc.target)
			defer targetDb.Close()
			if len(targetDb.GetAllUsers()) != 0 {
				t.Fatal("target expected to be empty")
			}
			if len(targetDb.GetAllApiKeys()) != 0 || len(targetDb.GetAllMetadata()) != 0 ||
				len(targetDb.GetAllFileRequests()) != 0 || len(targetDb.GetAllHotlinks()) != 0 {
				t.Fatal("target expected to contain no migrated records")
			}
			// Rerun is deterministic.
			if err := Run(tc.source, tc.target); err != nil {
				t.Fatalf("idempotent rerun failed: %v", err)
			}
		})
	}
}

func TestRerunIsIdempotent(t *testing.T) {
	source := sqliteConfig(t, "source")
	target := sqliteConfig(t, "target")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	runMigration(t, source, target)
	if err := Run(source, target); err != nil {
		t.Fatalf("rerun failed: %v", err)
	}

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb := openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "rerun")
}

func TestChangedSourceIsRefused(t *testing.T) {
	source := sqliteConfig(t, "source")
	target := sqliteConfig(t, "target")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	runMigration(t, source, target)

	// A new object appears in the source after the snapshot was pinned.
	sourceDb = openDB(t, source)
	sourceDb.SaveApiKey(models.ApiKey{Id: "newer-key", PublicId: "newer-pub", UserId: 5})
	sourceDb.Close()

	err := Run(source, target)
	if err == nil {
		t.Fatal("migration with a changed source was accepted")
	}

	// The activated generation stays untouched.
	targetDb := openDB(t, target)
	defer targetDb.Close()
	if _, ok := targetDb.GetApiKey("newer-key"); ok {
		t.Fatal("newer source content overwrote the activated generation")
	}
	if _, ok := targetDb.GetApiKey("key-admin"); !ok {
		t.Fatal("activated generation content was lost")
	}
}

func TestResumeCompletesPartialCopy(t *testing.T) {
	source := redisConfig("ut_resume_src_")
	target := sqliteConfig(t, "target")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	// Simulate a crashed process: state exists at phase "copying", but only
	// the users were staged.
	sourceDb = openDB(t, source)
	snap, err := readSnapshot(sourceDb)
	if err != nil {
		t.Fatal(err)
	}
	sourceDb.Close()

	targetDb := openDB(t, target)
	backend := targetDb.(StagingBackend)
	if err := backend.MigPrepare(); err != nil {
		t.Fatal(err)
	}
	checksum, err := snap.checksum()
	if err != nil {
		t.Fatal(err)
	}
	st, err := newState(snap, source.Type, target.Type, locationKey(source), locationKey(target), checksum)
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range snap.users {
		if err := backend.MigStageUser(st.MigrationId, user); err != nil {
			t.Fatal(err)
		}
	}
	blob, err := st.json()
	if err != nil {
		t.Fatal(err)
	}
	created, err := backend.MigCreateState(st.MigrationId, blob)
	if err != nil || !created {
		t.Fatalf("state creation returned created=%v err=%v", created, err)
	}
	targetDb.Close()

	// Re-running the command resumes the same generation and snapshot.
	runMigration(t, source, target)

	sourceDb = openDB(t, source)
	defer sourceDb.Close()
	targetDb = openDB(t, target)
	defer targetDb.Close()
	assertContentEqual(t, sourceDb, targetDb, "resume")
}

func TestTamperedStagingBlocksActivation(t *testing.T) {
	source := sqliteConfig(t, "source")
	target := redisConfig("ut_tamper_dst_")
	sourceDb := openDB(t, source)
	seedData(t, sourceDb)
	sourceDb.Close()

	// Target contains a previous usable generation that must survive.
	targetDb := openDB(t, target)
	seedStaleTargetData(t, targetDb, "target-session")

	// Create a fully copied generation, then corrupt one record and mark the
	// state as "copied", as if verification had never run.
	sourceDb = openDB(t, source)
	snap, err := readSnapshot(sourceDb)
	if err != nil {
		t.Fatal(err)
	}
	backend := targetDb.(StagingBackend)
	if err := backend.MigPrepare(); err != nil {
		t.Fatal(err)
	}
	checksum, err := snap.checksum()
	if err != nil {
		t.Fatal(err)
	}
	st, err := newState(snap, source.Type, target.Type, locationKey(source), locationKey(target), checksum)
	if err != nil {
		t.Fatal(err)
	}
	created, err := backend.MigCreateState(st.MigrationId, mustMarshalState(t, st))
	if err != nil || !created {
		t.Fatalf("state creation returned created=%v err=%v", created, err)
	}
	if err := copySnapshot(backend, st, snap); err != nil {
		t.Fatal(err)
	}
	corrupted := snap.files["file-hotlink"]
	corrupted.Name = "corrupted-name"
	if err := backend.MigStageFile(st.MigrationId, corrupted); err != nil {
		t.Fatal(err)
	}
	st.Phase = phaseCopied
	if err := backend.MigSaveStateCAS(st.MigrationId, mustMarshalState(t, st)); err != nil {
		t.Fatal(err)
	}
	sourceDb.Close()
	targetDb.Close()

	if err := Run(source, target); err == nil {
		t.Fatal("corrupted generation passed integrity verification")
	}

	// The previous usable generation is still live.
	targetDb = openDB(t, target)
	defer targetDb.Close()
	if _, ok := targetDb.GetUser(99); !ok {
		t.Fatal("previous generation was modified despite failed verification")
	}
	assertSessionPresent(t, targetDb, "target-session", "tamper")
	if _, ok := targetDb.GetApiKey("key-admin"); ok {
		t.Fatal("unverified generation became visible")
	}
}

func TestSourceReadFailureLeavesNoTarget(t *testing.T) {
	source := models.DbConnection{
		Type:        dbabstraction.TypeRedis,
		HostUrl:     "127.0.0.1:1",
		RedisPrefix: "ut_unreachable_",
	}
	target := sqliteConfig(t, "target")

	if err := Run(source, target); err == nil {
		t.Fatal("migration against an unreachable source succeeded")
	}
	if _, err := os.Stat(target.HostUrl); !os.IsNotExist(err) {
		t.Fatalf("destination database was created despite source failure: %v", err)
	}
}

// uncertainWriteBackend simulates an uncertain write response: the write
// actually reaches the database, but the client receives an error.
type uncertainWriteBackend struct {
	StagingBackend
	failed map[string]bool
}

func (b *uncertainWriteBackend) MigStageFile(genId string, file models.File) error {
	if !b.failed[file.Id] {
		b.failed[file.Id] = true
		if err := b.StagingBackend.MigStageFile(genId, file); err != nil {
			return err
		}
		return errors.New("simulated connection reset after write")
	}
	return b.StagingBackend.MigStageFile(genId, file)
}

func TestEnsureRecordRecoversFromUncertainWrite(t *testing.T) {
	target := sqliteConfig(t, "target")
	targetDb := openDB(t, target)
	backend := &uncertainWriteBackend{
		StagingBackend: targetDb.(StagingBackend),
		failed:         map[string]bool{},
	}
	if err := backend.MigPrepare(); err != nil {
		t.Fatal(err)
	}
	file := models.File{Id: "uncertain-file", Name: "name", UserId: 5}
	err := ensureRecord(backend, "gen1", kindFile, file.Id, file,
		backend.MigStageFile, backend.MigGetStagedFile)
	if err != nil {
		t.Fatalf("ensureRecord did not recover from uncertain write: %v", err)
	}
	// Second call confirms the record and does not rewrite.
	err = ensureRecord(backend, "gen1", kindFile, file.Id, file,
		backend.MigStageFile, backend.MigGetStagedFile)
	if err != nil {
		t.Fatalf("ensureRecord rerun failed: %v", err)
	}
	targetDb.Close()
}

func mustMarshalState(t *testing.T, st *state) []byte {
	t.Helper()
	blob, err := st.json()
	if err != nil {
		t.Fatal(err)
	}
	return blob
}
