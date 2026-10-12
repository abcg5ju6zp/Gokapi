package api

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/configuration"
	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/storage/chunking"
	"github.com/forceu/gokapi/internal/storage/chunking/chunkreservation"
	"github.com/forceu/gokapi/internal/storage/filerequest"
	"github.com/forceu/gokapi/internal/test"
)

// createFileRequestForTest creates a persisted file request with its dedicated API key
func createFileRequestForTest(t *testing.T, maxFiles, maxSize int) (models.FileRequest, models.ApiKey) {
	t.Helper()
	user, ok := database.GetUser(idUser)
	if !ok {
		t.Fatal("test user does not exist")
	}
	request := filerequest.New(user)
	request.MaxFiles = maxFiles
	request.MaxSize = maxSize
	key := generateNewKey(false, user.Id, "File Request Public Access", request.Id)
	request.ApiKey = key.Id
	database.SaveFileRequest(request)
	t.Cleanup(func() {
		chunkreservation.CancelAllForRequest(request.Id)
		for _, f := range filerequest.GetAllFiles(request) {
			database.DeleteMetaData(f.Id)
		}
		database.DeleteApiKey(key.Id)
		database.DeleteFileRequest(request)
	})
	return request, key
}

func reserveUploadId(t *testing.T, request models.FileRequest, key models.ApiKey, expectedStatus int) string {
	t.Helper()
	w, r := getRecorder("/api/uploadrequest/chunk/reserve", key.Id, []test.Header{
		{Name: "id", Value: request.Id},
	})
	Process(w, r)
	test.IsEqualInt(t, w.Code, expectedStatus)
	if w.Code != 200 {
		return ""
	}
	result := struct {
		Result string
		Uuid   string
	}{}
	test.IsNil(t, json.Unmarshal(w.Body.Bytes(), &result))
	test.IsEqualString(t, result.Result, "OK")
	test.IsEqualBool(t, len(result.Uuid) >= 10, true)
	return result.Uuid
}

func writeRequestChunk(t *testing.T, uuid string, request models.FileRequest, key models.ApiKey) *os.File {
	t.Helper()
	tmpFile, err := os.CreateTemp("test/data", "frupload")
	test.IsNil(t, err)
	_, err = tmpFile.WriteString("chunktestfile")
	test.IsNil(t, err)
	test.IsNil(t, tmpFile.Close())

	body, formContent := test.FileToMultipartFormBody(t, test.HttpTestConfig{
		UploadFileName:  tmpFile.Name(),
		UploadFieldName: "file",
		PostValues: []test.PostBody{
			{Key: "filesize", Value: "13"},
			{Key: "offset", Value: "0"},
			{Key: "uuid", Value: uuid},
		},
	})
	w, r := test.GetRecorder("POST", "/api/uploadrequest/chunk/add", nil, []test.Header{
		{Name: "apikey", Value: key.Id},
		{Name: "fileRequestId", Value: request.Id},
	}, body)
	r.Header.Add("Content-Type", formContent)
	Process(w, r)
	test.IsEqualInt(t, w.Code, 200)
	test.IsEqualString(t, w.Body.String(), `{"result":"OK"}`)
	return tmpFile
}

func completeRequestChunk(t *testing.T, uuid string, request models.FileRequest, key models.ApiKey, expectedStatus int) models.FileApiOutput {
	t.Helper()
	w, r := getRecorder("/api/uploadrequest/chunk/complete", key.Id, []test.Header{
		{Name: "uuid", Value: uuid},
		{Name: "filename", Value: "frtest.upload"},
		{Name: "filesize", Value: "13"},
		{Name: "fileRequestId", Value: request.Id},
	})
	Process(w, r)
	test.IsEqualInt(t, w.Code, expectedStatus)
	result := struct {
		FileInfo models.FileApiOutput `json:"FileInfo"`
	}{}
	body, _ := io.ReadAll(w.Body)
	test.IsNil(t, json.Unmarshal(body, &result))
	return result.FileInfo
}

// TestFileRequestUpload_HappyPath reserves, uploads and completes a file through the
// file request API and verifies that the slot is converted to "used" exactly once
func TestFileRequestUpload_HappyPath(t *testing.T) {
	request, key := createFileRequestForTest(t, 2, 25)

	uuid := reserveUploadId(t, request, key, 200)
	// the reservation occupies one persistent slot
	test.IsEqualInt(t, chunkreservation.GetCount(request.Id), 1)
	reservation, ok := chunkreservation.Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt64(t, reservation.Version, request.Version)

	tmpFile := writeRequestChunk(t, uuid, request, key)
	defer os.Remove(tmpFile.Name())

	// reservation advanced to uploading
	reservation, _ = chunkreservation.Get(uuid)
	test.IsEqualInt(t, reservation.Status, models.ReservationStatusUploading)

	fileInfo := completeRequestChunk(t, uuid, request, key, 200)
	test.IsEqualString(t, fileInfo.Name, "frtest.upload")

	// metadata is linked to the request
	meta, ok := database.GetMetaDataById(fileInfo.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualString(t, meta.UploadRequestId, request.Id)

	// completed reservations do not occupy a slot but are retained for idempotent retries
	test.IsEqualInt(t, chunkreservation.GetCount(request.Id), 0)
	reservation, _ = chunkreservation.Get(uuid)
	test.IsEqualInt(t, reservation.Status, models.ReservationStatusCompleted)
	test.IsEqualString(t, reservation.FileId, fileInfo.Id)

	// retrying the completion returns the same file, not a second one
	retryInfo := completeRequestChunk(t, uuid, request, key, 200)
	test.IsEqualString(t, retryInfo.Id, fileInfo.Id)

	// one file used of two allowed, a second slot is available
	populated, exists := filerequest.Get(request.Id)
	test.IsEqualBool(t, exists, true)
	test.IsEqualInt(t, populated.UploadedFiles, 1)
	test.IsEqualInt(t, populated.FilesRemaining(), 1)
}

// TestFileRequestUpload_LimitEnforced verifies that no more reservations than MaxFiles
// can be created, even with concurrent requests
func TestFileRequestUpload_LimitEnforced(t *testing.T) {
	request, key := createFileRequestForTest(t, 1, 25)
	uuid := reserveUploadId(t, request, key, 200)
	_ = reserveUploadId(t, request, key, 400)
	tmpFile := writeRequestChunk(t, uuid, request, key)
	defer os.Remove(tmpFile.Name())
	_ = completeRequestChunk(t, uuid, request, key, 200)
	// after completion, the used slot still counts against the limit
	_ = reserveUploadId(t, request, key, 401)
}

// TestFileRequestUpload_UnreserveReleasesSlot verifies explicit cancellation
func TestFileRequestUpload_UnreserveReleasesSlot(t *testing.T) {
	request, key := createFileRequestForTest(t, 1, 25)
	uuid := reserveUploadId(t, request, key, 200)
	_ = reserveUploadId(t, request, key, 400)

	w, r := getRecorder("/api/uploadrequest/chunk/unreserve", key.Id, []test.Header{
		{Name: "id", Value: request.Id},
		{Name: "uuid", Value: uuid},
	})
	Process(w, r)
	test.IsEqualInt(t, w.Code, 200)
	test.IsEqualString(t, w.Body.String(), `{"Result":"OK"}`)
	test.IsEqualInt(t, chunkreservation.GetCount(request.Id), 0)

	// slot available again
	_ = reserveUploadId(t, request, key, 200)
}

// TestFileRequestUpload_ChunkWithoutReservationRejected verifies that only reserved
// uploads can write chunks
func TestFileRequestUpload_ChunkWithoutReservationRejected(t *testing.T) {
	request, key := createFileRequestForTest(t, 5, 25)
	body, formContent := test.FileToMultipartFormBody(t, test.HttpTestConfig{
		UploadFileName:  "test/fileupload.jpg",
		UploadFieldName: "file",
		PostValues: []test.PostBody{
			{Key: "filesize", Value: "3"},
			{Key: "offset", Value: "0"},
			{Key: "uuid", Value: "never-reserved-xyz"},
		},
	})
	w, r := test.GetRecorder("POST", "/api/uploadrequest/chunk/add", nil, []test.Header{
		{Name: "apikey", Value: key.Id},
		{Name: "fileRequestId", Value: request.Id},
	}, body)
	r.Header.Add("Content-Type", formContent)
	Process(w, r)
	test.IsEqualInt(t, w.Code, 400)
}

// TestFileRequestUpload_EditInvalidatesReservation verifies that chunks and completion
// of an older request generation cannot advance after the request was edited
func TestFileRequestUpload_EditInvalidatesReservation(t *testing.T) {
	request, key := createFileRequestForTest(t, 5, 25)
	uuid := reserveUploadId(t, request, key, 200)

	// editing the request increases its generation
	request.Name = "renamed request"
	request.BumpVersion()
	database.SaveFileRequest(request)

	tmpFile, err := os.CreateTemp("test/data", "frupload")
	test.IsNil(t, err)
	_, _ = tmpFile.WriteString("chunktestfile")
	test.IsNil(t, tmpFile.Close())
	defer os.Remove(tmpFile.Name())

	body, formContent := test.FileToMultipartFormBody(t, test.HttpTestConfig{
		UploadFileName:  tmpFile.Name(),
		UploadFieldName: "file",
		PostValues: []test.PostBody{
			{Key: "filesize", Value: "13"},
			{Key: "offset", Value: "0"},
			{Key: "uuid", Value: uuid},
		},
	})
	w, r := test.GetRecorder("POST", "/api/uploadrequest/chunk/add", nil, []test.Header{
		{Name: "apikey", Value: key.Id},
		{Name: "fileRequestId", Value: request.Id},
	}, body)
	r.Header.Add("Content-Type", formContent)
	Process(w, r)
	test.IsEqualInt(t, w.Code, 400)

	// completion of the old generation is rejected as well
	w, r = getRecorder("/api/uploadrequest/chunk/complete", key.Id, []test.Header{
		{Name: "uuid", Value: uuid},
		{Name: "filename", Value: "frtest.upload"},
		{Name: "filesize", Value: "13"},
		{Name: "fileRequestId", Value: request.Id},
	})
	Process(w, r)
	test.IsEqualInt(t, w.Code, 400)

	// background cleanup releases the reservation of the old generation
	chunkreservation.Cleanup()
	test.IsEqualInt(t, chunkreservation.GetCount(request.Id), 0)
}

// TestFileRequestUpload_WrongKeyRejected verifies that a reservation is bound to the
// API key it was created with
func TestFileRequestUpload_WrongKeyRejected(t *testing.T) {
	request, key := createFileRequestForTest(t, 5, 25)
	uuid := reserveUploadId(t, request, key, 200)

	otherUser, _ := database.GetUser(idUser)
	otherRequest := filerequest.New(otherUser)
	otherRequest.MaxFiles = 5
	otherKey := generateNewKey(false, otherUser.Id, "File Request Public Access", otherRequest.Id)
	otherRequest.ApiKey = otherKey.Id
	database.SaveFileRequest(otherRequest)
	t.Cleanup(func() {
		database.DeleteApiKey(otherKey.Id)
		database.DeleteFileRequest(otherRequest)
	})

	// uploading to the request with a different key is rejected
	_, err := chunkreservation.MarkUploading(request.Id, uuid, otherKey.Id)
	test.IsEqualBool(t, err == chunkreservation.ErrReservationKey, true)
}

// TestFileRequestUpload_ObjectRecovery verifies that a completion whose storage object
// exists but whose metadata commit was lost is reconciled without creating a second file
func TestFileRequestUpload_ObjectRecovery(t *testing.T) {
	request, key := createFileRequestForTest(t, 1, 25)
	uuid := reserveUploadId(t, request, key, 200)
	tmpFile := writeRequestChunk(t, uuid, request, key)
	defer os.Remove(tmpFile.Name())

	header := chunking.FileHeader{Filename: "frtest.upload", ContentType: "text/plain", Size: 13}
	reservation, err := chunkreservation.Claim(request.Id, uuid, key.Id, header)
	test.IsNil(t, err)

	// simulate: object stored under its hash, chunk removed, metadata missing
	content := []byte("chunktestfile")
	hashSum := sha1.Sum(content)
	sha := hex.EncodeToString(hashSum[:])
	objectPath := configuration.Get().DataDir + "/" + sha
	test.IsNil(t, os.WriteFile(objectPath, content, 0600))
	test.IsNil(t, os.Remove(configuration.Get().DataDir + "/chunk-" + uuid))
	reservation.Sha1 = sha
	reservation.Updated = time.Now().Unix() - 11*60
	database.SaveUploadReservation(reservation)

	// the request still owns no confirmed file at this point
	_, metadataExists := database.GetMetaDataById(reservation.FileId)
	test.IsEqualBool(t, metadataExists, false)

	chunkreservation.Cleanup()

	completed, ok := chunkreservation.Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, completed.Status, models.ReservationStatusCompleted)

	// metadata was recovered against the existing object, no second file was created
	file, metadataExists := database.GetMetaDataById(reservation.FileId)
	test.IsEqualBool(t, metadataExists, true)
	test.IsEqualString(t, file.SHA1, sha)
	test.IsEqualString(t, file.Name, "frtest.upload")
	test.IsEqualInt(t, chunkreservation.GetCount(request.Id), 0)

	// a retry reports the same file id
	retryInfo := completeRequestChunk(t, uuid, request, key, 200)
	test.IsEqualString(t, retryInfo.Id, reservation.FileId)
}
