package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/forceu/gokapi/internal/models"
	redigo "github.com/gomodule/redigo/redis"
)

// Migration control keys (after the configured database prefix).
const (
	migStateKey      = "migration:state"
	migGenerationKey = "migration:gen"
	migNamespace     = "mig:"
)

// MigPrepare verifies that the destination is reachable. Staging uses regular
// keys in a dedicated namespace, so no schema setup is required.
func (p DatabaseProvider) MigPrepare() error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := redigo.String(conn.Do("PING"))
	return err
}

// MigrationStateExists probes a destination for an in-progress migration
// without writing anything (in particular without initialising dbversion).
func MigrationStateExists(config models.DbConnection) (blob []byte, exists bool, err error) {
	pool := newPool(config)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("redis connection failed: %v", recovered)
		}
		_ = pool.Close()
	}()
	conn := pool.Get()
	defer conn.Close()
	if _, err = redigo.String(conn.Do("PING")); err != nil {
		return nil, false, err
	}
	value, err := redigo.Bytes(conn.Do("GET", config.RedisPrefix+migStateKey))
	if errors.Is(err, redigo.ErrNil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (p DatabaseProvider) stageKey(genId, baseKey string) string {
	return p.dbPrefix + migNamespace + genId + ":" + baseKey
}

func (p DatabaseProvider) stageBaseKey(genId, baseKey string) string {
	return migNamespace + genId + ":" + baseKey
}

// MigLoadState returns the persisted migration state.
func (p DatabaseProvider) MigLoadState() ([]byte, bool, error) {
	conn := p.pool.Get()
	defer conn.Close()
	value, err := redigo.Bytes(conn.Do("GET", p.dbPrefix+migStateKey))
	if errors.Is(err, redigo.ErrNil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// stateCreateScript atomically stores the first state, only if none exists.
// KEYS[1] = generation key, KEYS[2] = state key
// ARGV[1] = migration id, ARGV[2] = state json
const stateCreateScript = `
if redis.call('GET', KEYS[1]) then
	return 0
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
return 1`

// MigCreateState stores the first migration state, failing if one exists.
func (p DatabaseProvider) MigCreateState(migrationId string, stateBlob []byte) (bool, error) {
	conn := p.pool.Get()
	defer conn.Close()
	result, err := redigo.Int(conn.Do("EVAL", stateCreateScript, 2,
		p.dbPrefix+migGenerationKey, p.dbPrefix+migStateKey,
		migrationId, stateBlob))
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

// stateSaveCassScript overwrites the state only if the stored generation id
// matches ARGV[1]. KEYS[1] = generation key, KEYS[2] = state key,
// ARGV[2] = state json.
const stateSaveCassScript = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
	return 0
end
redis.call('SET', KEYS[2], ARGV[2])
return 1`

// MigSaveStateCAS updates the state only for the current generation.
func (p DatabaseProvider) MigSaveStateCAS(expectedId string, stateBlob []byte) error {
	conn := p.pool.Get()
	defer conn.Close()
	result, err := redigo.Int(conn.Do("EVAL", stateSaveCassScript, 2,
		p.dbPrefix+migGenerationKey, p.dbPrefix+migStateKey,
		expectedId, stateBlob))
	if err != nil {
		return err
	}
	if result != 1 {
		return errors.New("migration state was modified by another process")
	}
	return nil
}

// MigStageUser stages a user.
func (p DatabaseProvider) MigStageUser(genId string, user models.User) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HMSET",
		p.buildArgs(p.stageBaseKey(genId, prefixUsers+strconv.Itoa(user.Id))).AddFlat(user)...)
	return err
}

// MigStageApiKey stages an API key.
func (p DatabaseProvider) MigStageApiKey(genId string, apiKey models.ApiKey) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HMSET",
		p.buildArgs(p.stageBaseKey(genId, prefixApiKeys+apiKey.Id)).AddFlat(apiKey)...)
	return err
}

// MigStageEnd2End stages encrypted end-to-end info.
func (p DatabaseProvider) MigStageEnd2End(genId string, userId int, info models.E2EInfoEncrypted) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HMSET",
		p.buildArgs(p.stageBaseKey(genId, idE2EInfo+strconv.Itoa(userId))).AddFlat(info)...)
	return err
}

// MigStageFile stages file metadata including the gob-encoded encryption info.
func (p DatabaseProvider) MigStageFile(genId string, file models.File) error {
	marshalled, err := marshalEncryptionInfo(file)
	if err != nil {
		return err
	}
	conn := p.pool.Get()
	defer conn.Close()
	_, err = conn.Do("HMSET",
		p.buildArgs(p.stageBaseKey(genId, prefixMetaData+file.Id)).AddFlat(marshalled)...)
	return err
}

// MigStageHotlink stages a hotlink mapping.
func (p DatabaseProvider) MigStageHotlink(genId string, hotlinkId string, fileId string) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", p.stageKey(genId, prefixHotlinks+hotlinkId), fileId)
	return err
}

// MigStageFileRequest stages a file request.
func (p DatabaseProvider) MigStageFileRequest(genId string, request models.FileRequest) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HMSET",
		p.buildArgs(p.stageBaseKey(genId, prefixFileRequests+request.Id)).AddFlat(request)...)
	return err
}

// MigStageUserIdMax stages the Redis user id counter.
func (p DatabaseProvider) MigStageUserIdMax(genId string, maxUserId int) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", p.stageKey(genId, prefixUserIdCounter), maxUserId)
	return err
}

// MigGetStagedUser reads a staged user.
func (p DatabaseProvider) MigGetStagedUser(genId string, userId int) (models.User, bool, error) {
	hash, ok := p.getHashMap(p.stageBaseKey(genId, prefixUsers+strconv.Itoa(userId)))
	if !ok {
		return models.User{}, false, nil
	}
	user, err := dbToUser(hash)
	return user, err == nil, err
}

// MigGetStagedApiKey reads a staged API key.
func (p DatabaseProvider) MigGetStagedApiKey(genId string, id string) (models.ApiKey, bool, error) {
	hash, ok := p.getHashMap(p.stageBaseKey(genId, prefixApiKeys+id))
	if !ok {
		return models.ApiKey{}, false, nil
	}
	apiKey, err := dbToApiKey(prefixApiKeys+id, hash)
	return apiKey, err == nil, err
}

// MigGetStagedEnd2End reads staged end-to-end info.
func (p DatabaseProvider) MigGetStagedEnd2End(genId string, userId int) (models.E2EInfoEncrypted, bool, error) {
	hash, ok := p.getHashMap(p.stageBaseKey(genId, idE2EInfo+strconv.Itoa(userId)))
	if !ok {
		return models.E2EInfoEncrypted{}, false, nil
	}
	var result models.E2EInfoEncrypted
	if err := redigo.ScanStruct(hash, &result); err != nil {
		return models.E2EInfoEncrypted{}, false, err
	}
	return result, true, nil
}

// MigGetStagedFile reads staged file metadata.
func (p DatabaseProvider) MigGetStagedFile(genId string, id string) (models.File, bool, error) {
	hash, ok := p.getHashMap(p.stageBaseKey(genId, prefixMetaData+id))
	if !ok {
		return models.File{}, false, nil
	}
	file, err := dbToMetadata(prefixMetaData+id, hash)
	return file, err == nil, err
}

// MigGetStagedHotlink reads a staged hotlink mapping.
func (p DatabaseProvider) MigGetStagedHotlink(genId string, id string) (string, bool, error) {
	conn := p.pool.Get()
	defer conn.Close()
	value, err := redigo.String(conn.Do("GET", p.stageKey(genId, prefixHotlinks+id)))
	if errors.Is(err, redigo.ErrNil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

// MigGetStagedFileRequest reads a staged file request.
func (p DatabaseProvider) MigGetStagedFileRequest(genId string, id string) (models.FileRequest, bool, error) {
	hash, ok := p.getHashMap(p.stageBaseKey(genId, prefixFileRequests+id))
	if !ok {
		return models.FileRequest{}, false, nil
	}
	request, err := dbToFileRequest(hash)
	return request, err == nil, err
}

// MigGetStagedUserIdMax reads the staged user id counter.
func (p DatabaseProvider) MigGetStagedUserIdMax(genId string) (int, bool, error) {
	conn := p.pool.Get()
	defer conn.Close()
	value, err := redigo.Int(conn.Do("GET", p.stageKey(genId, prefixUserIdCounter)))
	if errors.Is(err, redigo.ErrNil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// MigStagedCounts returns the number of staged records per kind.
func (p DatabaseProvider) MigStagedCounts(genId string) (map[string]int, error) {
	result := map[string]int{}
	for _, baseKey := range p.getAllKeysWithPrefix(migNamespace + genId + ":") {
		switch {
		case strings.Contains(baseKey, ":"+prefixUsers):
			result["user"]++
		case strings.Contains(baseKey, ":"+prefixApiKeys):
			result["apikey"]++
		case strings.Contains(baseKey, ":"+prefixMetaData):
			result["file"]++
		case strings.Contains(baseKey, ":"+prefixHotlinks):
			result["hotlink"]++
		case strings.Contains(baseKey, ":"+prefixFileRequests):
			result["filerequest"]++
		case strings.Contains(baseKey, ":"+idE2EInfo):
			result["e2e"]++
		case strings.HasSuffix(baseKey, ":"+prefixUserIdCounter):
			// counter is not a record kind
		}
	}
	return result, nil
}

// activationScript atomically publishes a staging generation:
//   - live keys of migrated prefixes that are not part of the generation are
//     deleted (old data is removed without mixing),
//   - staging keys are renamed over the live key names,
//   - expiring API keys get their expiry restored,
//   - the schema version is set.
//
// Session (se:) and statistics (stats:) keys are never touched.
const activationScript = `
local pos = 2
local function takeList()
	local n = tonumber(ARGV[pos]); pos = pos + 1
	local t = {}
	for i = 1, n do t[i] = ARGV[pos]; pos = pos + 1 end
	return t, n
end
local live, nLive = takeList()
local stage, nStage = takeList()
local prefixes = takeList()
local keep = {}
for i = 1, nLive do keep[live[i]] = true end
for i = 1, #prefixes do
	local keys = redis.call('KEYS', prefixes[i])
	for j = 1, #keys do
		if not keep[keys[j]] then redis.call('DEL', keys[j]) end
	end
end
local uidKey = ARGV[1] .. 'userid_max'
if not keep[uidKey] then redis.call('DEL', uidKey) end
for i = 1, nStage do
	redis.call('DEL', live[i])
	redis.call('RENAME', stage[i], live[i])
end
local nExp = tonumber(ARGV[pos]); pos = pos + 1
for i = 1, nExp do
	local key = ARGV[pos]; local value = ARGV[pos + 1]; pos = pos + 2
	redis.call('EXPIREAT', key, value)
end
redis.call('SET', ARGV[1] .. 'dbversion', ARGV[pos])
return 'OK'`

// MigActivate publishes the staging generation atomically.
func (p DatabaseProvider) MigActivate(genId string, schemaVersion int, apiKeyExpiries map[string]int64) error {
	stageBases := p.getAllKeysWithPrefix(migNamespace + genId + ":")
	liveKeys := make([]string, 0, len(stageBases))
	stageKeys := make([]string, 0, len(stageBases))
	for _, base := range stageBases {
		liveBase := strings.TrimPrefix(base, migNamespace+genId+":")
		liveKeys = append(liveKeys, p.dbPrefix+liveBase)
		stageKeys = append(stageKeys, p.dbPrefix+base)
	}

	stalePrefixes := []string{
		p.dbPrefix + prefixUsers + "*",
		p.dbPrefix + prefixApiKeys + "*",
		p.dbPrefix + prefixMetaData + "*",
		p.dbPrefix + prefixHotlinks + "*",
		p.dbPrefix + prefixFileRequests + "*",
		p.dbPrefix + idE2EInfo + "*",
	}

	expiryKeys := make([]string, 0, 2*len(apiKeyExpiries))
	ids := make([]string, 0, len(apiKeyExpiries))
	for id := range apiKeyExpiries {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		expiryKeys = append(expiryKeys, p.dbPrefix+prefixApiKeys+id, strconv.FormatInt(apiKeyExpiries[id], 10))
	}

	args := redigo.Args{}
	args = args.Add(p.dbPrefix)
	args = args.Add(len(liveKeys)).AddFlat(liveKeys)
	args = args.Add(len(stageKeys)).AddFlat(stageKeys)
	args = args.Add(len(stalePrefixes)).AddFlat(stalePrefixes)
	args = args.Add(len(apiKeyExpiries)).AddFlat(expiryKeys)
	args = args.Add(schemaVersion)

	evalArgs := redigo.Args{activationScript, 0}
	evalArgs = append(evalArgs, []interface{}(args)...)

	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("EVAL", []interface{}(evalArgs)...)
	return err
}

// MigDropStaging removes all staging keys of the generation.
func (p DatabaseProvider) MigDropStaging(genId string) error {
	conn := p.pool.Get()
	defer conn.Close()
	baseKeys := p.getAllKeysWithPrefix(migNamespace + genId + ":")
	if len(baseKeys) == 0 {
		return nil
	}
	args := redigo.Args{}
	for _, base := range baseKeys {
		args = args.Add(p.dbPrefix + base)
	}
	_, err := conn.Do("DEL", args...)
	return err
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j-1] > values[j]; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}
