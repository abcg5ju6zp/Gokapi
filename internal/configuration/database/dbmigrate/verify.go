package dbmigrate

import (
	"fmt"
	"strconv"

	"github.com/forceu/gokapi/internal/models"
)

// stagedContent holds all records read back from a staged generation.
type stagedContent struct {
	users    []models.User
	e2e      map[int]models.E2EInfoEncrypted
	apiKeys  map[string]models.ApiKey
	files    map[string]models.File
	hotlinks map[string]string
	requests []models.FileRequest
}

// verifyStaging reads every staged record back, proves that each one matches
// the pinned source manifest hash and that all associations between the
// records are intact. It returns the recomputed overall checksum on success.
func verifyStaging(backend StagingBackend, genId string, sourceVersion int, m manifest) (string, error) {
	counts, err := backend.MigStagedCounts(genId)
	if err != nil {
		return "", fmt.Errorf("unable to count staged records: %w", err)
	}
	for _, kind := range allKinds {
		if counts[kind] != m.Counts[kind] {
			return "", fmt.Errorf("integrity check failed: expected %d staged %s records, found %d",
				m.Counts[kind], kind, counts[kind])
		}
	}

	staged := stagedContent{
		e2e:      make(map[int]models.E2EInfoEncrypted),
		apiKeys:  make(map[string]models.ApiKey),
		files:    make(map[string]models.File),
		hotlinks: make(map[string]string),
	}
	hashes := make(map[string]string, len(m.Hashes))

	for hashKey, expectedHash := range m.Hashes {
		kind, id, err := parseHashKey(hashKey)
		if err != nil {
			return "", err
		}
		value, err := readStagedRecord(backend, genId, kind, id)
		if err != nil {
			return "", err
		}
		actualHash, err := recordHash(kind, value)
		if err != nil {
			return "", err
		}
		if actualHash != expectedHash {
			return "", fmt.Errorf("integrity check failed: staged %s %s does not match source snapshot", kind, id)
		}
		hashes[hashKey] = actualHash
		collectStagedRecord(&staged, kind, id, value)
	}

	if err := verifyStagedRelations(staged); err != nil {
		return "", err
	}

	// The user id counter only exists on backends that maintain one (Redis);
	// SQLite stores explicit user ids and reports no counter.
	userIdMax, ok, err := backend.MigGetStagedUserIdMax(genId)
	if err != nil {
		return "", fmt.Errorf("unable to read staged user id counter: %w", err)
	}
	if ok && userIdMax != m.UserIdMax {
		return "", fmt.Errorf("integrity check failed: staged user id counter %d does not match %d", userIdMax, m.UserIdMax)
	}

	return checksumFromHashes(sourceVersion, hashes), nil
}

func parseHashKey(hashKey string) (string, string, error) {
	for i := 0; i < len(hashKey); i++ {
		if hashKey[i] == ':' {
			return hashKey[:i], hashKey[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("invalid manifest key: %s", hashKey)
}

func readStagedRecord(backend StagingBackend, genId, kind, id string) (any, error) {
	switch kind {
	case kindUser:
		userId, err := strconv.Atoi(id)
		if err != nil {
			return nil, err
		}
		value, ok, err := backend.MigGetStagedUser(genId, userId)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged user %d", userId)
		}
		return value, nil
	case kindE2E:
		userId, err := strconv.Atoi(id)
		if err != nil {
			return nil, err
		}
		value, ok, err := backend.MigGetStagedEnd2End(genId, userId)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged end-to-end info %d", userId)
		}
		return value, nil
	case kindApiKey:
		value, ok, err := backend.MigGetStagedApiKey(genId, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged API key %s", id)
		}
		return value, nil
	case kindFile:
		value, ok, err := backend.MigGetStagedFile(genId, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged file %s", id)
		}
		return value, nil
	case kindHotlink:
		value, ok, err := backend.MigGetStagedHotlink(genId, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged hotlink %s", id)
		}
		return value, nil
	case kindFileRequest:
		value, ok, err := backend.MigGetStagedFileRequest(genId, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("integrity check failed: missing staged file request %s", id)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown record kind %s", kind)
	}
}

func collectStagedRecord(staged *stagedContent, kind, id string, value any) {
	switch kind {
	case kindUser:
		staged.users = append(staged.users, value.(models.User))
	case kindE2E:
		userId, _ := strconv.Atoi(id)
		staged.e2e[userId] = value.(models.E2EInfoEncrypted)
	case kindApiKey:
		staged.apiKeys[id] = value.(models.ApiKey)
	case kindFile:
		staged.files[id] = value.(models.File)
	case kindHotlink:
		staged.hotlinks[id] = value.(string)
	case kindFileRequest:
		staged.requests = append(staged.requests, value.(models.FileRequest))
	}
}

// verifyStagedRelations proves that hotlinks, end-to-end info and API keys are
// still bound to the objects they belong to after the copy.
func verifyStagedRelations(s stagedContent) error {
	userIds := make(map[int]bool, len(s.users))
	maxId := 0
	for _, user := range s.users {
		userIds[user.Id] = true
		if user.Id > maxId {
			maxId = user.Id
		}
	}
	requestIds := make(map[string]bool, len(s.requests))
	requestByApiKey := make(map[string]string)
	for _, request := range s.requests {
		requestIds[request.Id] = true
		if request.ApiKey != "" {
			requestByApiKey[request.ApiKey] = request.Id
		}
	}

	for hotlinkId, fileId := range s.hotlinks {
		file, ok := s.files[fileId]
		if !ok {
			return fmt.Errorf("integrity check failed: hotlink %s points to missing file %s", hotlinkId, fileId)
		}
		if file.HotlinkId != hotlinkId {
			return fmt.Errorf("integrity check failed: hotlink %s is not registered at file %s", hotlinkId, fileId)
		}
	}
	for _, file := range s.files {
		if file.HotlinkId != "" && s.hotlinks[file.HotlinkId] != file.Id {
			return fmt.Errorf("integrity check failed: file %s references unregistered hotlink %s", file.Id, file.HotlinkId)
		}
		if file.UploadRequestId != "" && !requestIds[file.UploadRequestId] {
			return fmt.Errorf("integrity check failed: file %s references missing file request %s", file.Id, file.UploadRequestId)
		}
	}
	for userId := range s.e2e {
		if !userIds[userId] {
			return fmt.Errorf("integrity check failed: end-to-end info references missing owner %d", userId)
		}
	}
	for _, request := range s.requests {
		if !userIds[request.UserId] {
			return fmt.Errorf("integrity check failed: file request %s references missing owner %d", request.Id, request.UserId)
		}
		if request.ApiKey != "" {
			key, ok := s.apiKeys[request.ApiKey]
			if !ok {
				return fmt.Errorf("integrity check failed: file request %s references missing API key %s", request.Id, request.ApiKey)
			}
			if key.UploadRequestId != request.Id {
				return fmt.Errorf("integrity check failed: API key %s is not bound to file request %s", request.ApiKey, request.Id)
			}
		}
	}
	for id, key := range s.apiKeys {
		if key.UploadRequestId != "" {
			if !requestIds[key.UploadRequestId] {
				return fmt.Errorf("integrity check failed: API key %s references missing file request %s", id, key.UploadRequestId)
			}
			if requestByApiKey[id] != key.UploadRequestId {
				return fmt.Errorf("integrity check failed: file request %s is not bound to API key %s", key.UploadRequestId, id)
			}
		}
	}
	return nil
}
