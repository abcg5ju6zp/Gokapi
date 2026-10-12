package redis

import (
	"fmt"
	"strconv"
	"time"

	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	redigo "github.com/gomodule/redigo/redis"
)

const (
	// prefixReservation stores a single reservation hash at urs:<uuid>
	prefixReservation = "urs:"
	// prefixReservationActive is a set per file request containing the uuids of
	// reservations that still occupy a slot (status reserved/uploading/finishing)
	prefixReservationActive = "urq:"
	// prefixReservationUsedFiles is a set per file request containing the ids of
	// confirmed file metadata that count towards the file limit
	prefixReservationUsedFiles = "urf:"
)

// createReservationScript atomically validates the file request generation/key/expiry
// and occupies a slot if one is available.
//
// KEYS[1] = frq:<requestId>, KEYS[2] = urs:<uuid>, KEYS[3] = urq:<requestId>, KEYS[4] = urf:<requestId>
// ARGV: uuid, maxFiles, version, apiKey, now, expiry, maxSizeBytes, requestId, created
//
// Returns 1 on success, 0 if no slot is available and a negative value on a validation error.
const createReservationScript = `
local req = redis.call('HGETALL', KEYS[1])
if #req == 0 then return -1 end
for i = 1, #req, 2 do
	if req[i] == 'version' and tostring(req[i+1]) ~= ARGV[3] then return -2 end
	if req[i] == 'apikey' and tostring(req[i+1]) ~= ARGV[4] then return -3 end
end
local expiry = redis.call('HGET', KEYS[1], 'expiry')
if expiry ~= false and expiry ~= nil and tonumber(expiry) ~= nil and tonumber(expiry) > 0 and tonumber(expiry) < tonumber(ARGV[5]) then
	return -4
end
local maxFiles = tonumber(ARGV[2])
if maxFiles ~= nil and maxFiles > 0 then
	if redis.call('SCARD', KEYS[4]) + redis.call('SCARD', KEYS[3]) >= maxFiles then
		return 0
	end
end
if redis.call('EXISTS', KEYS[2]) == 1 then return -5 end
redis.call('HMSET', KEYS[2],
	'uuid', ARGV[1],
	'filerequestid', ARGV[8],
	'version', ARGV[3],
	'apikeyid', ARGV[4],
	'maxsizebytes', ARGV[7],
	'status', 0,
	'expiry', ARGV[6],
	'created', ARGV[9],
	'updated', ARGV[9],
	'fileid', '')
redis.call('SADD', KEYS[3], ARGV[1])
return 1
`

// casReservationScript atomically transitions a reservation status.
//
// KEYS[1] = urs:<uuid>, KEYS[2] = urq:<requestId>
// ARGV: newStatus, newExpiry, fileId, now, uuid, allowedStatus...
//
// Returns 1 if the transition was performed, 0 otherwise.
const casReservationScript = `
local cur = redis.call('HGET', KEYS[1], 'status')
if not cur then return 0 end
local match = false
for i = 6, #ARGV do
	if tostring(cur) == ARGV[i] then match = true end
end
if not match then return 0 end
redis.call('HSET', KEYS[1], 'status', ARGV[1], 'expiry', ARGV[2], 'updated', ARGV[4])
if ARGV[3] ~= '' then
	redis.call('HSET', KEYS[1], 'fileid', ARGV[3])
end
if tonumber(ARGV[1]) <= 2 then
	redis.call('SADD', KEYS[2], ARGV[5])
else
	redis.call('SREM', KEYS[2], ARGV[5])
end
return 1
`

// GetUploadReservation returns the reservation for the given upload id
func (p DatabaseProvider) GetUploadReservation(uuid string) (models.UploadReservation, bool) {
	if uuid == "" {
		return models.UploadReservation{}, false
	}
	result, ok := p.getHashMap(prefixReservation + uuid)
	if !ok {
		return models.UploadReservation{}, false
	}
	var reservation models.UploadReservation
	helper.Check(redigo.ScanStruct(result, &reservation))
	return reservation, true
}

// GetAllUploadReservations returns all stored upload reservations
func (p DatabaseProvider) GetAllUploadReservations() []models.UploadReservation {
	result := make([]models.UploadReservation, 0)
	for _, v := range p.getAllHashesWithPrefix(prefixReservation) {
		var reservation models.UploadReservation
		helper.Check(redigo.ScanStruct(v, &reservation))
		result = append(result, reservation)
	}
	return result
}

// CountActiveUploadReservations returns the number of reservations that still occupy a slot
func (p DatabaseProvider) CountActiveUploadReservations(fileRequestId string) int {
	conn := p.pool.Get()
	defer conn.Close()
	count, err := redigo.Int(conn.Do("SCARD", p.dbPrefix+prefixReservationActive+fileRequestId))
	if err != nil {
		return 0
	}
	return count
}

// TryCreateUploadReservation atomically occupies a slot if the request still has room
func (p DatabaseProvider) TryCreateUploadReservation(reservation models.UploadReservation, maxFiles int) bool {
	conn := p.pool.Get()
	defer conn.Close()
	now := strconv.FormatInt(time.Now().Unix(), 10)
	args := redigo.Args{createReservationScript, 4}.
		Add(p.dbPrefix+prefixFileRequests+reservation.FileRequestId).
		Add(p.dbPrefix+prefixReservation+reservation.Uuid).
		Add(p.dbPrefix+prefixReservationActive+reservation.FileRequestId).
		Add(p.dbPrefix+prefixReservationUsedFiles+reservation.FileRequestId).
		Add(reservation.Uuid, strconv.Itoa(maxFiles), reservation.Version, reservation.ApiKeyId,
			now, reservation.Expiry, reservation.MaxSizeBytes, reservation.FileRequestId, now)
	result, err := redigo.Int(conn.Do("EVAL", []interface{}(args)...))
	if err != nil {
		fmt.Println("Warning, could not create upload reservation: " + err.Error())
		return false
	}
	if result < 0 {
		fmt.Printf("Warning, rejected upload reservation %s with code %d\n", reservation.Uuid, result)
		return false
	}
	return result == 1
}

// SaveUploadReservation stores the reservation unconditionally (used for recovery)
func (p DatabaseProvider) SaveUploadReservation(reservation models.UploadReservation) {
	p.setHashMap(p.buildArgs(prefixReservation + reservation.Uuid).AddFlat(reservation))
	conn := p.pool.Get()
	defer conn.Close()
	if reservation.IsActive() {
		_, _ = conn.Do("SADD", p.dbPrefix+prefixReservationActive+reservation.FileRequestId, reservation.Uuid)
	} else {
		_, _ = conn.Do("SREM", p.dbPrefix+prefixReservationActive+reservation.FileRequestId, reservation.Uuid)
	}
}

// CompareAndSetReservationStatus atomically transitions the status if the current one is allowed
func (p DatabaseProvider) CompareAndSetReservationStatus(uuid string, allowedFromStatuses []int, newStatus int, newExpiry int64, fileId string) bool {
	if len(allowedFromStatuses) == 0 {
		return false
	}
	reservation, ok := p.GetUploadReservation(uuid)
	if !ok {
		return false
	}
	conn := p.pool.Get()
	defer conn.Close()
	args := redigo.Args{casReservationScript, 2}.
		Add(p.dbPrefix+prefixReservation+uuid,
			p.dbPrefix+prefixReservationActive+reservation.FileRequestId).
		Add(newStatus, newExpiry, fileId, time.Now().Unix(), uuid)
	for _, status := range allowedFromStatuses {
		args = args.Add(status)
	}
	result, err := redigo.Int(conn.Do("EVAL", []interface{}(args)...))
	if err != nil {
		fmt.Println("Warning, could not update upload reservation: " + err.Error())
		return false
	}
	return result == 1
}

// DeleteUploadReservation deletes the reservation with the given uuid
func (p DatabaseProvider) DeleteUploadReservation(uuid string) {
	reservation, ok := p.GetUploadReservation(uuid)
	if !ok {
		return
	}
	conn := p.pool.Get()
	defer conn.Close()
	_, _ = conn.Do("SREM", p.dbPrefix+prefixReservationActive+reservation.FileRequestId, uuid)
	_, _ = conn.Do("DEL", p.dbPrefix+prefixReservation+uuid)
}

// DeleteUploadReservationsForRequest deletes all reservations of the given file request
func (p DatabaseProvider) DeleteUploadReservationsForRequest(fileRequestId string) {
	conn := p.pool.Get()
	defer conn.Close()
	activeKey := p.dbPrefix + prefixReservationActive + fileRequestId
	uuids, err := redigo.Strings(conn.Do("SMEMBERS", activeKey))
	if err == nil {
		for _, uuid := range uuids {
			_, _ = conn.Do("DEL", p.dbPrefix+prefixReservation+uuid)
		}
	}
	_, _ = conn.Do("DEL", activeKey)
	_, _ = conn.Do("DEL", p.dbPrefix+prefixReservationUsedFiles+fileRequestId)
	// Terminal (completed/cancelled) reservations are no longer members of the active set
	for key := range p.getAllHashesWithPrefix(prefixReservation) {
		uuid := key[len(prefixReservation):]
		if reservation, ok := p.GetUploadReservation(uuid); ok && reservation.FileRequestId == fileRequestId {
			_, _ = conn.Do("DEL", p.dbPrefix+prefixReservation+uuid)
		}
	}
}

// rebuildUsedFileSets repopulates the per-request used-file counters, e.g. after an upgrade
func (p DatabaseProvider) rebuildUsedFileSets() {
	for _, file := range p.GetAllMetadata() {
		if file.UploadRequestId == "" || file.PendingDeletion != 0 {
			continue
		}
		conn := p.pool.Get()
		_, _ = conn.Do("SADD", p.dbPrefix+prefixReservationUsedFiles+file.UploadRequestId, file.Id)
		conn.Close()
	}
}
