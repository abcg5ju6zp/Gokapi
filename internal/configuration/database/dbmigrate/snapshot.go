package dbmigrate

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"github.com/forceu/gokapi/internal/configuration/database/dbabstraction"
	"github.com/forceu/gokapi/internal/models"
)

// snapshot contains the complete, immutable content captured from the source
// database at migration start.
type snapshot struct {
	version int

	users    []models.User
	e2e      map[int]models.E2EInfoEncrypted
	apiKeys  map[string]models.ApiKey
	files    map[string]models.File
	hotlinks map[string]string // hotlinkId -> fileId
	requests []models.FileRequest

	userIdMax      int
	apiKeyExpiries map[string]int64
}

// readSnapshot reads all migratable objects of the source database in one
// pass. The snapshot is fully captured before the destination is touched, so
// a source read failure can never leave a usable partial target.
// Provider methods panic on database errors via helper.Check; those panics
// are converted into errors here.
func readSnapshot(src dbabstraction.Database) (snapshot, error) {
	result := snapshot{
		e2e:            make(map[int]models.E2EInfoEncrypted),
		apiKeys:        make(map[string]models.ApiKey),
		files:          make(map[string]models.File),
		hotlinks:       make(map[string]string),
		apiKeyExpiries: make(map[string]int64),
	}

	version, err := safeCall(src.GetDbVersion)
	if err != nil {
		return snapshot{}, err
	}
	result.version = version

	users, err := safeCall(src.GetAllUsers)
	if err != nil {
		return snapshot{}, err
	}
	result.users = users
	for _, user := range users {
		if user.Id > result.userIdMax {
			result.userIdMax = user.Id
		}
		info, err := safeCall(func() models.E2EInfoEncrypted { return src.GetEnd2EndInfo(user.Id) })
		if err != nil {
			return snapshot{}, err
		}
		if info.HasBeenSetUp() {
			result.e2e[user.Id] = info
		}
	}

	apiKeys, err := safeCall(src.GetAllApiKeys)
	if err != nil {
		return snapshot{}, err
	}
	result.apiKeys = apiKeys
	for id, key := range apiKeys {
		if key.Expiry != 0 {
			result.apiKeyExpiries[id] = key.Expiry
		}
	}

	requests, err := safeCall(src.GetAllFileRequests)
	if err != nil {
		return snapshot{}, err
	}
	result.requests = requests

	files, err := safeCall(src.GetAllMetadata)
	if err != nil {
		return snapshot{}, err
	}
	result.files = files

	hotlinkIds, err := safeCall(src.GetAllHotlinks)
	if err != nil {
		return snapshot{}, err
	}
	for _, hotlinkId := range hotlinkIds {
		fileId, ok, err := safeCall3(func() (string, bool) { return src.GetHotlink(hotlinkId) })
		if err != nil {
			return snapshot{}, err
		}
		if !ok {
			return snapshot{}, fmt.Errorf("source integrity error: hotlink %s has no file mapping", hotlinkId)
		}
		result.hotlinks[hotlinkId] = fileId
	}

	if err := result.validateRelations(); err != nil {
		return snapshot{}, err
	}
	return result, nil
}

// validateRelations ensures associations inside the source snapshot are
// consistent. The same checks run again against the staged generation.
func (s snapshot) validateRelations() error {
	for hotlinkId, fileId := range s.hotlinks {
		file, ok := s.files[fileId]
		if !ok {
			return fmt.Errorf("source integrity error: hotlink %s points to missing file %s", hotlinkId, fileId)
		}
		if file.HotlinkId != hotlinkId {
			return fmt.Errorf("source integrity error: hotlink %s is not registered at file %s", hotlinkId, fileId)
		}
	}
	for _, file := range s.files {
		if file.HotlinkId == "" {
			continue
		}
		if s.hotlinks[file.HotlinkId] != file.Id {
			return fmt.Errorf("source integrity error: file %s references unregistered hotlink %s", file.Id, file.HotlinkId)
		}
		if file.UploadRequestId != "" {
			if _, ok := s.requestById(file.UploadRequestId); !ok {
				return fmt.Errorf("source integrity error: file %s references missing file request %s", file.Id, file.UploadRequestId)
			}
		}
	}
	for _, request := range s.requests {
		if _, ok := s.userById(request.UserId); !ok {
			return fmt.Errorf("source integrity error: file request %s references missing owner %d", request.Id, request.UserId)
		}
		if request.ApiKey != "" {
			key, ok := s.apiKeys[request.ApiKey]
			if !ok {
				return fmt.Errorf("source integrity error: file request %s references missing API key %s", request.Id, request.ApiKey)
			}
			if key.UploadRequestId != request.Id {
				return fmt.Errorf("source integrity error: API key %s is not bound to file request %s", request.ApiKey, request.Id)
			}
		}
	}
	for id, key := range s.apiKeys {
		if key.UploadRequestId != "" {
			request, ok := s.requestById(key.UploadRequestId)
			if !ok {
				return fmt.Errorf("source integrity error: API key %s references missing file request %s", id, key.UploadRequestId)
			}
			if request.ApiKey != id {
				return fmt.Errorf("source integrity error: file request %s is not bound to API key %s", key.UploadRequestId, id)
			}
		}
	}
	for userId := range s.e2e {
		if _, ok := s.userById(userId); !ok {
			return fmt.Errorf("source integrity error: end-to-end info references missing owner %d", userId)
		}
	}
	return nil
}

func (s snapshot) userById(id int) (models.User, bool) {
	for _, user := range s.users {
		if user.Id == id {
			return user, true
		}
	}
	return models.User{}, false
}

func (s snapshot) requestById(id string) (models.FileRequest, bool) {
	for _, request := range s.requests {
		if request.Id == id {
			return request, true
		}
	}
	return models.FileRequest{}, false
}

// hashEntries returns the sorted "kind:id=hash" lines of every record in the
// snapshot. The ordering makes the overall fingerprint deterministic.
func (s snapshot) hashEntries() ([]string, error) {
	lines := make([]string, 0, len(s.e2e)+len(s.apiKeys)+len(s.files)+len(s.hotlinks)+len(s.requests)+len(s.users))
	for _, user := range s.users {
		hash, err := recordHash(kindUser, user)
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindUser+":"+strconv.Itoa(user.Id)+"="+hash)
	}
	// Sort users by id, as GetAllUsers order is by rank.
	sort.Strings(lines)
	e2eIds := make([]int, 0, len(s.e2e))
	for userId := range s.e2e {
		e2eIds = append(e2eIds, userId)
	}
	sort.Ints(e2eIds)
	for _, userId := range e2eIds {
		hash, err := recordHash(kindE2E, s.e2e[userId])
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindE2E+":"+strconv.Itoa(userId)+"="+hash)
	}
	for _, id := range sortedKeys(s.apiKeys) {
		hash, err := recordHash(kindApiKey, s.apiKeys[id])
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindApiKey+":"+id+"="+hash)
	}
	for _, id := range sortedKeys(s.files) {
		hash, err := recordHash(kindFile, s.files[id])
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindFile+":"+id+"="+hash)
	}
	for _, id := range sortedKeys(s.hotlinks) {
		hash, err := recordHash(kindHotlink, s.hotlinks[id])
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindHotlink+":"+id+"="+hash)
	}
	requestIds := make([]string, 0, len(s.requests))
	requestMap := make(map[string]models.FileRequest, len(s.requests))
	for _, request := range s.requests {
		requestIds = append(requestIds, request.Id)
		requestMap[request.Id] = request
	}
	sort.Strings(requestIds)
	for _, id := range requestIds {
		hash, err := recordHash(kindFileRequest, requestMap[id])
		if err != nil {
			return nil, err
		}
		lines = append(lines, kindFileRequest+":"+id+"="+hash)
	}
	return lines, nil
}

// checksum builds the fingerprint of the complete snapshot, including the
// pinned source database version.
func (s snapshot) checksum() (string, error) {
	lines, err := s.hashEntries()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "version=%d\n", s.version)
	for _, line := range lines {
		h.Write([]byte(line))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s snapshot) toManifest() manifest {
	m := manifest{
		Counts:         map[string]int{},
		Hashes:         map[string]string{},
		UserIdMax:      s.userIdMax,
		ApiKeyExpiries: s.apiKeyExpiries,
	}
	m.Counts[kindUser] = len(s.users)
	m.Counts[kindE2E] = len(s.e2e)
	m.Counts[kindApiKey] = len(s.apiKeys)
	m.Counts[kindFile] = len(s.files)
	m.Counts[kindHotlink] = len(s.hotlinks)
	m.Counts[kindFileRequest] = len(s.requests)
	lines, err := s.hashEntries()
	if err != nil {
		// hashing only fails on gob encoding errors, which are impossible for
		// the models used here.
		panic(err)
	}
	for _, line := range lines {
		sep := 0
		for i := 0; i < len(line); i++ {
			if line[i] == '=' {
				sep = i
				break
			}
		}
		m.Hashes[line[:sep]] = line[sep+1:]
	}
	return m
}

// checksumFromHashes recomputes the overall snapshot fingerprint from a set
// of per-record hashes, used during integrity verification.
func checksumFromHashes(sourceVersion int, hashes map[string]string) string {
	keys := make([]string, 0, len(hashes))
	for key := range hashes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	fmt.Fprintf(h, "version=%d\n", sourceVersion)
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{'='})
		h.Write([]byte(hashes[key]))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// recordHash returns a content hash for a single staged/snapshot record.
func recordHash(kind string, value any) (string, error) {
	var buf bytes.Buffer
	buf.WriteString(kind)
	buf.WriteByte(0)
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return "", fmt.Errorf("unable to hash %s record: %w", kind, err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// safeCall converts a provider panic (helper.Check panics on database errors)
// into a regular error.
func safeCall[T any](fn func() T) (result T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("source database read failed: %v", recovered)
		}
	}()
	return fn(), nil
}

// safeCall3 is safeCall for functions returning (value, bool).
func safeCall3[T any](fn func() (T, bool)) (result T, ok bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("source database read failed: %v", recovered)
		}
	}()
	result, ok = fn()
	return result, ok, nil
}
