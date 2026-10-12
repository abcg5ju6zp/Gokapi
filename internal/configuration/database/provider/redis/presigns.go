package redis

import (
	"strconv"

	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	redigo "github.com/gomodule/redigo/redis"
)

const (
	prefixPresigns = "psign:"
	// presignSafetyTtl keeps an upper bound on the key lifetime in Redis.
	// Expired credentials are also removed periodically by RecoverAndClean.
	presignSafetyTtl int64 = 24 * 60 * 60
)

// claimScript atomically claims an available, unexpired credential.
// KEYS[1] = credential key, ARGV[1] = now (unix), ARGV[2] = claim token
var claimScript = `
local status = redis.call('HGET', KEYS[1], 'Status')
if status == false then return 0 end
if tonumber(status) ~= 0 then return 0 end
local expiry = redis.call('HGET', KEYS[1], 'Expiry')
if expiry == false or tonumber(expiry) < tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'Status', 1, 'ClaimedAt', ARGV[1], 'ClaimToken', ARGV[2])
return 1`

// releaseScript returns a claimed credential to the available state, provided
// the caller still owns the claim token. ARGV[1] = claim token
var releaseScript = `
local token = redis.call('HGET', KEYS[1], 'ClaimToken')
if token == false or token ~= ARGV[1] then return 0 end
local status = redis.call('HGET', KEYS[1], 'Status')
if status == false or tonumber(status) ~= 1 then return 0 end
redis.call('HSET', KEYS[1], 'Status', 0, 'ClaimedAt', 0, 'ClaimToken', '')
return 1`

// deliverScript moves a claimed credential to the terminal delivered state,
// provided the caller still owns the claim token. ARGV[1] = claim token
var deliverScript = `
local token = redis.call('HGET', KEYS[1], 'ClaimToken')
if token == false or token ~= ARGV[1] then return 0 end
local status = redis.call('HGET', KEYS[1], 'Status')
if status == false or tonumber(status) ~= 1 then return 0 end
redis.call('HSET', KEYS[1], 'Status', 2)
return 1`

func (p DatabaseProvider) dbToPresign(id string, input []any) (models.Presign, error) {
	var result models.Presign
	err := redigo.ScanStruct(input, &result)
	if err != nil {
		return models.Presign{}, err
	}
	result.Id = id
	err = result.UnmarshalFiles()
	return result, err
}

// GetPresign returns the presigned download credential with the given ID
func (p DatabaseProvider) GetPresign(id string) (models.Presign, bool) {
	if id == "" {
		return models.Presign{}, false
	}
	result, ok := p.getHashMap(prefixPresigns + id)
	if !ok {
		return models.Presign{}, false
	}
	presign, err := p.dbToPresign(id, result)
	helper.Check(err)
	return presign, true
}

// GetAllPresigns returns all persisted presigned download credentials
func (p DatabaseProvider) GetAllPresigns() []models.Presign {
	var result []models.Presign
	maps := p.getAllHashesWithPrefix(prefixPresigns)
	for key, value := range maps {
		id := key[len(prefixPresigns):]
		presign, err := p.dbToPresign(id, value)
		if err != nil {
			helper.Check(err)
			continue
		}
		result = append(result, presign)
	}
	return result
}

// SavePresign stores a presigned download credential
func (p DatabaseProvider) SavePresign(presign models.Presign) {
	err := presign.MarshalFiles()
	helper.Check(err)
	p.setHashMap(p.buildArgs(prefixPresigns + presign.Id).AddFlat(presign))
	p.setExpiryAt(prefixPresigns+presign.Id, presign.Expiry+presignSafetyTtl)
}

// DeletePresign deletes a presigned download credential with the given ID
func (p DatabaseProvider) DeletePresign(id string) {
	p.deleteKey(prefixPresigns + id)
}

func (p DatabaseProvider) runPresignScript(script string, id string, extraArgs ...any) bool {
	conn := p.pool.Get()
	defer conn.Close()
	args := redigo.Args{script, 1, p.dbPrefix + prefixPresigns + id}.AddFlat(extraArgs)
	result, err := redigo.Int(conn.Do("EVAL", args...))
	if err != nil {
		helper.Check(err)
		return false
	}
	return result == 1
}

// ClaimPresign atomically transitions an available, unexpired credential to the claimed state
func (p DatabaseProvider) ClaimPresign(id string, now int64, token string) (models.Presign, bool) {
	if !p.runPresignScript(claimScript, id, strconv.FormatInt(now, 10), token) {
		return models.Presign{}, false
	}
	return p.GetPresign(id)
}

// ReleasePresign atomically returns a claimed credential to the available state
func (p DatabaseProvider) ReleasePresign(id string, token string) bool {
	return p.runPresignScript(releaseScript, id, token)
}

// SetPresignDelivered atomically transitions a claimed credential to the terminal delivered state
func (p DatabaseProvider) SetPresignDelivered(id string, token string) bool {
	return p.runPresignScript(deliverScript, id, token)
}
