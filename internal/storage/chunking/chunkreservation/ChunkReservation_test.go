package chunkreservation

import (
	"os"
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/configuration"
	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/storage/chunking"
	"github.com/forceu/gokapi/internal/test"
	"github.com/forceu/gokapi/internal/test/testconfiguration"
)

func TestMain(m *testing.M) {
	testconfiguration.Create(false)
	configuration.Load()
	configuration.ConnectDatabase()
	exitVal := m.Run()
	testconfiguration.Delete()
	os.Exit(exitVal)
}

func createTestRequest(t *testing.T, maxFiles int) models.FileRequest {
	t.Helper()
	request := models.FileRequest{
		Id:           helper.GenerateRandomString(20),
		UserId:       7,
		MaxFiles:     maxFiles,
		MaxSize:      100,
		CreationDate: time.Now().Unix(),
		Version:      1,
		ApiKey:       helper.GenerateRandomString(30),
		Name:         "test request",
	}
	database.SaveFileRequest(request)
	t.Cleanup(func() {
		CancelAllForRequest(request.Id)
		database.DeleteFileRequest(request)
	})
	return request
}

func TestNew_PersistsReservation(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 1024)
	test.IsNil(t, err)
	test.IsEqualInt(t, len(uuid), 32)
	test.IsEqualInt(t, GetCount(request.Id), 1)

	reservation, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualString(t, reservation.FileRequestId, request.Id)
	test.IsEqualString(t, reservation.ApiKeyId, request.ApiKey)
	test.IsEqualInt64(t, reservation.Version, 1)
	test.IsEqualInt64(t, reservation.MaxSizeBytes, 1024)
	test.IsEqualInt(t, reservation.Status, models.ReservationStatusReserved)
}

func TestNew_AtomicSlotLimit(t *testing.T) {
	request := createTestRequest(t, 1)
	_, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	_, err = New(request, request.ApiKey, 0)
	test.IsEqualBool(t, err == ErrNoSlotLeft, true)
	test.IsEqualInt(t, GetCount(request.Id), 1)
}

func TestNew_UnlimitedFiles(t *testing.T) {
	request := createTestRequest(t, 0)
	for i := 0; i < 5; i++ {
		_, err := New(request, request.ApiKey, 0)
		test.IsNil(t, err)
	}
	test.IsEqualInt(t, GetCount(request.Id), 5)
}

func TestNew_StaleRequestGenerationRejected(t *testing.T) {
	request := createTestRequest(t, 5)
	stale := request
	stale.Version = 42
	_, err := New(stale, request.ApiKey, 0)
	test.IsNotNil(t, err)
	test.IsEqualInt(t, GetCount(request.Id), 0)
}

func TestReservation_SurvivesRestart(t *testing.T) {
	request := createTestRequest(t, 3)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	// Reservations are read straight from the database, so a new process observes the same count
	test.IsEqualInt(t, GetCount(request.Id), 1)
	_, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
}

func TestMarkUploading(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 2048)
	test.IsNil(t, err)

	reservation, err := MarkUploading(request.Id, uuid, request.ApiKey)
	test.IsNil(t, err)
	test.IsEqualInt(t, reservation.Status, models.ReservationStatusUploading)
	test.IsEqualInt64(t, reservation.MaxSizeBytes, 2048)
	// deadline is extended close to the 23h upload deadline
	test.IsEqualBool(t, reservation.Expiry > time.Now().Unix()+timeReservationWithUpload-10, true)
	test.IsEqualInt(t, GetCount(request.Id), 1)

	// idempotent while uploading
	_, err = MarkUploading(request.Id, uuid, request.ApiKey)
	test.IsNil(t, err)

	// unknown uuid rejected
	_, err = MarkUploading(request.Id, "does-not-exist", request.ApiKey)
	test.IsNotNil(t, err)

	// wrong api key rejected
	_, err = MarkUploading(request.Id, uuid, "other-key")
	test.IsEqualBool(t, err == ErrReservationKey, true)

	// wrong file request rejected
	_, err = MarkUploading("other-request", uuid, request.ApiKey)
	test.IsNotNil(t, err)
}

func TestMarkUploading_RequestEditedRejected(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	request.BumpVersion()
	database.SaveFileRequest(request)
	_, err = MarkUploading(request.Id, uuid, request.ApiKey)
	test.IsEqualBool(t, err == ErrRequestVersion, true)
}

func TestMarkUploading_KeyRotatedRejected(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	request.ApiKey = "rotated-key"
	database.SaveFileRequest(request)
	_, err = MarkUploading(request.Id, uuid, "rotated-key")
	test.IsEqualBool(t, err == ErrReservationKey || err == ErrRequestVersion, true)
}

func TestClaimAndComplete(t *testing.T) {
	request := createTestRequest(t, 2)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	_, err = MarkUploading(request.Id, uuid, request.ApiKey)
	test.IsNil(t, err)

	reservation, err := Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{
		Filename: "test.upload", ContentType: "text/plain", Size: 13,
	})
	test.IsNil(t, err)
	test.IsEqualInt(t, reservation.Status, models.ReservationStatusFinishing)
	test.IsEqualBool(t, reservation.FileId != "", true)
	test.IsEqualString(t, reservation.FileName, "test.upload")
	test.IsEqualInt64(t, reservation.FileSize, 13)
	test.IsEqualInt(t, GetCount(request.Id), 1)

	// a second claim cannot finish the same reservation
	_, err = Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{})
	test.IsEqualBool(t, err == ErrAlreadyProcessing, true)

	// simulate the confirmed file metadata
	database.SaveMetaData(models.File{
		Id: reservation.FileId, Name: "test.file", UploadRequestId: request.Id,
		UnlimitedDownloads: true, UnlimitedTime: true, UserId: request.UserId,
	})
	ok := MarkCompleted(models.UploadReservation{Uuid: uuid, FileRequestId: request.Id, FileId: reservation.FileId})
	test.IsEqualBool(t, ok, true)
	// completed reservations do not occupy a slot anymore
	test.IsEqualInt(t, GetCount(request.Id), 0)

	// retry sees the already completed reservation
	_, err = Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{})
	test.IsEqualBool(t, err == ErrAlreadyCompleted, true)

	// exactly once: second completion is a no-op
	ok = MarkCompleted(models.UploadReservation{Uuid: uuid, FileRequestId: request.Id, FileId: reservation.FileId})
	test.IsEqualBool(t, ok, false)

	database.DeleteMetaData(reservation.FileId)
}

func TestResumeForRetry(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	reservation, err := Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{
		Filename: "test.upload", Size: 13,
	})
	test.IsNil(t, err)
	test.IsEqualBool(t, ResumeForRetry(uuid), true)
	reopened, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, reopened.Status, models.ReservationStatusUploading)
	test.IsEqualString(t, reopened.FileId, reservation.FileId)
	// only finishing reservations can be resumed
	test.IsEqualBool(t, ResumeForRetry(uuid), false)
}

func TestCancel(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)

	// another request cannot cancel it
	test.IsEqualBool(t, Cancel("other-request", uuid, request.ApiKey, false), false)
	test.IsEqualInt(t, GetCount(request.Id), 1)

	// wrong key cannot cancel it
	test.IsEqualBool(t, Cancel(request.Id, uuid, "other-key", false), false)

	test.IsEqualBool(t, Cancel(request.Id, uuid, request.ApiKey, false), true)
	test.IsEqualInt(t, GetCount(request.Id), 0)
	// idempotent
	test.IsEqualBool(t, Cancel(request.Id, uuid, request.ApiKey, false), true)
}

func TestCancel_RemovesChunk(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	chunkPath := configuration.Get().DataDir + "/chunk-" + uuid
	test.IsNil(t, os.WriteFile(chunkPath, []byte("chunkdata"), 0600))
	test.IsEqualBool(t, Cancel(request.Id, uuid, request.ApiKey, true), true)
	_, err = os.Stat(chunkPath)
	test.IsEqualBool(t, os.IsNotExist(err), true)
}

func TestCleanup_ExpiredReservationsReleased(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	chunkPath := configuration.Get().DataDir + "/chunk-" + uuid
	test.IsNil(t, os.WriteFile(chunkPath, []byte("chunkdata"), 0600))

	// force expiry
	reservation, _ := Get(uuid)
	reservation.Expiry = time.Now().Unix() - 1
	database.SaveUploadReservation(reservation)

	Cleanup()
	_, ok := Get(uuid)
	test.IsEqualBool(t, ok, false)
	_, err = os.Stat(chunkPath)
	test.IsEqualBool(t, os.IsNotExist(err), true)
	test.IsEqualInt(t, GetCount(request.Id), 0)
}

func TestCleanup_ValidReservationsKept(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	Cleanup()
	_, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
}

func TestCleanup_RequestDeletedReleases(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	database.DeleteFileRequest(request)
	Cleanup()
	_, ok := Get(uuid)
	test.IsEqualBool(t, ok, false)
}

func TestCleanup_StuckFinishingReopened(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	chunkPath := configuration.Get().DataDir + "/chunk-" + uuid
	test.IsNil(t, os.WriteFile(chunkPath, []byte("chunkdata"), 0600))

	reservation, err := Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{})
	test.IsNil(t, err)
	// simulate a crash during finishing
	reservation.Updated = time.Now().Unix() - finishingGrace - 10
	database.SaveUploadReservation(reservation)

	Cleanup()
	reopened, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, reopened.Status, models.ReservationStatusUploading)
}

func TestCleanup_StuckFinishingWithMetadataCompleted(t *testing.T) {
	request := createTestRequest(t, 1)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	reservation, err := Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{})
	test.IsNil(t, err)
	database.SaveMetaData(models.File{
		Id: reservation.FileId, Name: "test.file", UploadRequestId: request.Id,
		UnlimitedDownloads: true, UnlimitedTime: true, UserId: request.UserId,
	})
	reservation.Updated = time.Now().Unix() - finishingGrace - 10
	database.SaveUploadReservation(reservation)

	Cleanup()
	completed, ok := Get(uuid)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, completed.Status, models.ReservationStatusCompleted)
	// the confirmed file is preserved
	_, fileExists := database.GetMetaDataById(reservation.FileId)
	test.IsEqualBool(t, fileExists, true)
	test.IsEqualInt(t, GetCount(request.Id), 0)
	database.DeleteMetaData(reservation.FileId)
}

func TestCleanup_StuckFinishingWithoutChunkReleased(t *testing.T) {
	request := createTestRequest(t, 5)
	uuid, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	reservation, err := Claim(request.Id, uuid, request.ApiKey, chunking.FileHeader{})
	test.IsNil(t, err)
	reservation.Updated = time.Now().Unix() - finishingGrace - 10
	database.SaveUploadReservation(reservation)

	Cleanup()
	_, ok := Get(uuid)
	test.IsEqualBool(t, ok, false)
}

func TestCancelAllForRequest(t *testing.T) {
	request := createTestRequest(t, 0)
	uuid1, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	uuid2, err := New(request, request.ApiKey, 0)
	test.IsNil(t, err)
	CancelAllForRequest(request.Id)
	test.IsEqualInt(t, GetCount(request.Id), 0)
	_, ok := Get(uuid1)
	test.IsEqualBool(t, ok, false)
	_, ok = Get(uuid2)
	test.IsEqualBool(t, ok, false)
}
