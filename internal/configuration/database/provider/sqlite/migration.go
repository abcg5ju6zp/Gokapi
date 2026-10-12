package sqlite

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
)

// Migration staging kinds. Their values must match the kind identifiers used
// by the dbmigrate engine; the provider cannot import that package to avoid
// an import cycle.
const (
	migKindUser        = "user"
	migKindE2E         = "e2e"
	migKindApiKey      = "apikey"
	migKindFile        = "file"
	migKindHotlink     = "hotlink"
	migKindFileRequest = "filerequest"
)

const migrationDdl = `CREATE TABLE IF NOT EXISTS MigrationState (
	"Id" INTEGER NOT NULL PRIMARY KEY CHECK(Id = 1),
	"MigrationId" TEXT NOT NULL,
	"Content" BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS MigrationStaging (
	"Gen" TEXT NOT NULL,
	"Kind" TEXT NOT NULL,
	"ObjId" TEXT NOT NULL,
	"Content" BLOB NOT NULL,
	PRIMARY KEY("Gen","Kind","ObjId")
) WITHOUT ROWID;`

// MigPrepare creates the migration control and staging tables.
func (p DatabaseProvider) MigPrepare() error {
	if p.sqliteDb == nil {
		return errors.New("sqlite not initialised")
	}
	_, err := p.sqliteDb.Exec(migrationDdl)
	return err
}

// MigrationStateExists probes a destination for an in-progress migration
// without creating or modifying the database. A missing file or control table
// reports false and no error, so an unreachable source never leaves an empty
// destination behind.
func MigrationStateExists(config models.DbConnection) ([]byte, bool, error) {
	exists, err := helper.FileExists(config.HostUrl)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	provider, err := New(config)
	if err != nil {
		return nil, false, err
	}
	defer provider.Close()
	var content []byte
	row := provider.sqliteDb.QueryRow("SELECT Content FROM MigrationState WHERE Id = 1")
	err = row.Scan(&content)
	if errors.Is(err, sql.ErrNoRows) || (err != nil && strings.Contains(err.Error(), "no such table")) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return content, true, nil
}

// MigLoadState returns the persisted migration state.
func (p DatabaseProvider) MigLoadState() ([]byte, bool, error) {
	var content []byte
	row := p.sqliteDb.QueryRow("SELECT Content FROM MigrationState WHERE Id = 1")
	err := row.Scan(&content)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return content, true, nil
}

// MigCreateState stores the first migration state, failing if one already exists.
func (p DatabaseProvider) MigCreateState(migrationId string, stateBlob []byte) (bool, error) {
	result, err := p.sqliteDb.Exec(
		"INSERT OR IGNORE INTO MigrationState (Id, MigrationId, Content) VALUES (1, ?, ?)",
		migrationId, stateBlob)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

// MigSaveStateCAS updates the state only if the stored generation matches.
func (p DatabaseProvider) MigSaveStateCAS(expectedId string, stateBlob []byte) error {
	result, err := p.sqliteDb.Exec(
		"UPDATE MigrationState SET Content = ? WHERE Id = 1 AND MigrationId = ?",
		stateBlob, expectedId)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return errors.New("migration state was modified by another process")
	}
	return nil
}

func (p DatabaseProvider) stageRecord(gen, kind, id string, value any) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	_, err := p.sqliteDb.Exec(
		`INSERT OR REPLACE INTO MigrationStaging (Gen, Kind, ObjId, Content) VALUES (?, ?, ?, ?)`,
		gen, kind, id, buf.Bytes())
	return err
}

func (p DatabaseProvider) getStagedRecord(gen, kind, id string, target any) (bool, error) {
	var content []byte
	row := p.sqliteDb.QueryRow(
		"SELECT Content FROM MigrationStaging WHERE Gen = ? AND Kind = ? AND ObjId = ?",
		gen, kind, id)
	err := row.Scan(&content)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(target); err != nil {
		return false, err
	}
	return true, nil
}

// MigStageUser stages a user.
func (p DatabaseProvider) MigStageUser(genId string, user models.User) error {
	return p.stageRecord(genId, migKindUser, fmt.Sprintf("%d", user.Id), user)
}

// MigStageApiKey stages an API key.
func (p DatabaseProvider) MigStageApiKey(genId string, apiKey models.ApiKey) error {
	return p.stageRecord(genId, migKindApiKey, apiKey.Id, apiKey)
}

// MigStageEnd2End stages encrypted end-to-end info.
func (p DatabaseProvider) MigStageEnd2End(genId string, userId int, info models.E2EInfoEncrypted) error {
	return p.stageRecord(genId, migKindE2E, fmt.Sprintf("%d", userId), info)
}

// MigStageFile stages file metadata.
func (p DatabaseProvider) MigStageFile(genId string, file models.File) error {
	return p.stageRecord(genId, migKindFile, file.Id, file)
}

// MigStageHotlink stages a hotlink mapping.
func (p DatabaseProvider) MigStageHotlink(genId string, hotlinkId string, fileId string) error {
	return p.stageRecord(genId, migKindHotlink, hotlinkId, fileId)
}

// MigStageFileRequest stages a file request.
func (p DatabaseProvider) MigStageFileRequest(genId string, request models.FileRequest) error {
	return p.stageRecord(genId, migKindFileRequest, request.Id, request)
}

// MigStageUserIdMax is a no-op for SQLite, which stores explicit user ids.
func (p DatabaseProvider) MigStageUserIdMax(genId string, maxUserId int) error {
	return nil
}

// MigGetStagedUser reads a staged user.
func (p DatabaseProvider) MigGetStagedUser(genId string, userId int) (models.User, bool, error) {
	var result models.User
	exists, err := p.getStagedRecord(genId, migKindUser, fmt.Sprintf("%d", userId), &result)
	return result, exists, err
}

// MigGetStagedApiKey reads a staged API key.
func (p DatabaseProvider) MigGetStagedApiKey(genId string, id string) (models.ApiKey, bool, error) {
	var result models.ApiKey
	exists, err := p.getStagedRecord(genId, migKindApiKey, id, &result)
	return result, exists, err
}

// MigGetStagedEnd2End reads staged end-to-end info.
func (p DatabaseProvider) MigGetStagedEnd2End(genId string, userId int) (models.E2EInfoEncrypted, bool, error) {
	var result models.E2EInfoEncrypted
	exists, err := p.getStagedRecord(genId, migKindE2E, fmt.Sprintf("%d", userId), &result)
	return result, exists, err
}

// MigGetStagedFile reads staged file metadata.
func (p DatabaseProvider) MigGetStagedFile(genId string, id string) (models.File, bool, error) {
	var result models.File
	exists, err := p.getStagedRecord(genId, migKindFile, id, &result)
	return result, exists, err
}

// MigGetStagedHotlink reads a staged hotlink mapping.
func (p DatabaseProvider) MigGetStagedHotlink(genId string, id string) (string, bool, error) {
	var result string
	exists, err := p.getStagedRecord(genId, migKindHotlink, id, &result)
	return result, exists, err
}

// MigGetStagedFileRequest reads a staged file request.
func (p DatabaseProvider) MigGetStagedFileRequest(genId string, id string) (models.FileRequest, bool, error) {
	var result models.FileRequest
	exists, err := p.getStagedRecord(genId, migKindFileRequest, id, &result)
	return result, exists, err
}

// MigGetStagedUserIdMax always reports 0 for SQLite.
func (p DatabaseProvider) MigGetStagedUserIdMax(genId string) (int, bool, error) {
	return 0, false, nil
}

// MigStagedCounts returns the number of staged records per kind.
func (p DatabaseProvider) MigStagedCounts(genId string) (map[string]int, error) {
	result := map[string]int{}
	rows, err := p.sqliteDb.Query(
		"SELECT Kind, COUNT(*) FROM MigrationStaging WHERE Gen = ? GROUP BY Kind", genId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var count int
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, err
		}
		result[kind] = count
	}
	return result, rows.Err()
}

// MigActivate publishes the staging generation inside a single transaction:
// all migrated live tables are replaced with the staged content. Sessions and
// statistics tables are never touched. On any error the transaction is rolled
// back and the previous live data remains in place.
func (p DatabaseProvider) MigActivate(genId string, schemaVersion int, apiKeyExpiries map[string]int64) error {
	tx, err := p.sqliteDb.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := activateStagedUsers(p, tx, genId); err != nil {
		return err
	}
	if err := activateStagedApiKeys(tx, genId); err != nil {
		return err
	}
	if err := activateStagedEnd2End(tx, genId); err != nil {
		return err
	}
	if err := activateStagedFileRequests(tx, genId); err != nil {
		return err
	}
	if err := activateStagedFiles(tx, genId); err != nil {
		return err
	}
	if err := activateStagedHotlinks(tx, genId); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	// PRAGMA user_version is not transactional, so it is set after the commit.
	p.SetDbVersion(schemaVersion)
	return nil
}

// MigDropStaging removes the staging rows of the generation.
func (p DatabaseProvider) MigDropStaging(genId string) error {
	_, err := p.sqliteDb.Exec("DELETE FROM MigrationStaging WHERE Gen = ?", genId)
	return err
}

const selectStagedContent = "SELECT Content FROM MigrationStaging WHERE Gen = ? AND Kind = ?"

func parseStagedUserId(objId string) (int, error) {
	userId, err := strconv.Atoi(objId)
	if err != nil {
		return 0, fmt.Errorf("invalid staged user id %q: %w", objId, err)
	}
	return userId, nil
}

func activateStagedUsers(p DatabaseProvider, tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM Users"); err != nil {
		return err
	}
	rows, err := tx.Query(selectStagedContent, genId, migKindUser)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return err
		}
		var user models.User
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&user); err != nil {
			return err
		}
		resetPassword := 0
		if user.ResetPassword {
			resetPassword = 1
		}
		if _, err := tx.Exec(
			"INSERT INTO Users (Id, Name, Password, Permissions, Userlevel, LastOnline, ResetPassword) VALUES (?, ?, ?, ?, ?, ?, ?)",
			user.Id, user.Name, user.Password, user.Permissions, user.UserLevel, user.LastOnline, resetPassword); err != nil {
			return err
		}
	}
	return rows.Err()
}

func activateStagedApiKeys(tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM ApiKeys"); err != nil {
		return err
	}
	rows, err := tx.Query(selectStagedContent, genId, migKindApiKey)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return err
		}
		var apiKey models.ApiKey
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&apiKey); err != nil {
			return err
		}
		isSystemKey := 0
		if apiKey.IsSystemKey {
			isSystemKey = 1
		}
		if _, err := tx.Exec(
			`INSERT INTO ApiKeys (Id, FriendlyName, LastUsed, Permissions, Expiry, IsSystemKey, UserId, PublicId, UploadRequestId)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			apiKey.Id, apiKey.FriendlyName, apiKey.LastUsed, apiKey.Permissions, apiKey.Expiry,
			isSystemKey, apiKey.UserId, apiKey.PublicId, apiKey.UploadRequestId); err != nil {
			return err
		}
	}
	return rows.Err()
}

func activateStagedEnd2End(tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM E2EConfig"); err != nil {
		return err
	}
	rows, err := tx.Query(
		"SELECT ObjId, Content FROM MigrationStaging WHERE Gen = ? AND Kind = ?",
		genId, migKindE2E)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var objId string
		var content []byte
		if err := rows.Scan(&objId, &content); err != nil {
			return err
		}
		userId, err := parseStagedUserId(objId)
		if err != nil {
			return err
		}
		var info models.E2EInfoEncrypted
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&info); err != nil {
			return err
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(info); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO E2EConfig (Config, UserId) VALUES (?, ?)", buf.Bytes(), userId); err != nil {
			return err
		}
	}
	return rows.Err()
}

func activateStagedFileRequests(tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM UploadRequests"); err != nil {
		return err
	}
	rows, err := tx.Query(selectStagedContent, genId, migKindFileRequest)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return err
		}
		var request models.FileRequest
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&request); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO UploadRequests (id, name, userid, expiry, maxFiles, maxSize, creation, apiKey, note)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			request.Id, request.Name, request.UserId, request.Expiry, request.MaxFiles, request.MaxSize,
			request.CreationDate, request.ApiKey, request.Notes); err != nil {
			return err
		}
	}
	return rows.Err()
}

func activateStagedFiles(tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM FileMetaData"); err != nil {
		return err
	}
	rows, err := tx.Query(selectStagedContent, genId, migKindFile)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			return err
		}
		var file models.File
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&file); err != nil {
			return err
		}
		unlimitedDownloads := 0
		if file.UnlimitedDownloads {
			unlimitedDownloads = 1
		}
		unlimitedTime := 0
		if file.UnlimitedTime {
			unlimitedTime = 1
		}
		var encryption bytes.Buffer
		if err := gob.NewEncoder(&encryption).Encode(file.Encryption); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`INSERT INTO FileMetaData (Id, Name, Size, SHA1, ExpireAt, SizeBytes,
				DownloadsRemaining, DownloadCount, PasswordHash, HotlinkId, ContentType, AwsBucket, Encryption,
				UnlimitedDownloads, UnlimitedTime, UserId, UploadDate, PendingDeletion, UploadRequestId)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			file.Id, file.Name, file.Size, file.SHA1, file.ExpireAt, file.SizeBytes,
			file.DownloadsRemaining, file.DownloadCount, file.PasswordHash, file.HotlinkId, file.ContentType,
			file.AwsBucket, encryption.Bytes(), unlimitedDownloads, unlimitedTime, file.UserId,
			file.UploadDate, file.PendingDeletion, file.UploadRequestId); err != nil {
			return err
		}
	}
	return rows.Err()
}

func activateStagedHotlinks(tx *sql.Tx, genId string) error {
	if _, err := tx.Exec("DELETE FROM Hotlinks"); err != nil {
		return err
	}
	rows, err := tx.Query(
		"SELECT ObjId, Content FROM MigrationStaging WHERE Gen = ? AND Kind = ?",
		genId, migKindHotlink)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var hotlinkId string
		var content []byte
		if err := rows.Scan(&hotlinkId, &content); err != nil {
			return err
		}
		var fileId string
		if err := gob.NewDecoder(bytes.NewBuffer(content)).Decode(&fileId); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO Hotlinks (Id, FileId) VALUES (?, ?)", hotlinkId, fileId); err != nil {
			return err
		}
	}
	return rows.Err()
}
