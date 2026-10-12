package chunkreservation

import (
	"errors"
	"sync"
	"time"

	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/environment"
	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/storage/chunking"
)

const (
	// timeReservationWithoutUpload is the deadline of a reservation that never received a chunk
	timeReservationWithoutUpload = 4 * 60
	// timeReservationWithUpload is the deadline after the first chunk was written
	timeReservationWithUpload = 23 * 60 * 60
	// timeReservationCompleted keeps terminal completed reservations for idempotent retries
	timeReservationCompleted = 24 * 60 * 60
	// finishingGrace is the time a completion may stay in "finishing" before it is reconciled,
	// e.g. after a crash while the storage or database response was uncertain
	finishingGrace = 10 * 60
	cleanupEvery   = 5 * time.Minute
)

// Possible reservation errors
var (
	// ErrNoSlotLeft is returned when the file request has reached its maximum number of files
	ErrNoSlotLeft = errors.New("no more files can be uploaded for this file request")
	// ErrReservationInvalid is returned when the reservation does not exist or is in the wrong state
	ErrReservationInvalid = errors.New("chunk reservation is invalid")
	// ErrReservationExpired is returned when the reservation deadline passed
	ErrReservationExpired = errors.New("chunk reservation has expired")
	// ErrRequestVersion is returned when the request was edited or its API key rotated after the reservation
	ErrRequestVersion = errors.New("file request has been modified since the upload was started")
	// ErrRequestExpired is returned when the file request expired
	ErrRequestExpired = errors.New("file request has expired")
	// ErrReservationKey is returned when the reservation belongs to a different API key
	ErrReservationKey = errors.New("chunk reservation was created with a different API key")
	// ErrAlreadyProcessing is returned while another completion is already claiming the reservation
	ErrAlreadyProcessing = errors.New("upload is already being processed, please retry")
	// ErrAlreadyCompleted is returned (together with the reservation) for idempotent retries
	ErrAlreadyCompleted = errors.New("upload has already been completed")
)

var startCleanupOnce sync.Once

// recoverFileMetadata is registered by the storage package (to avoid an import cycle)
// and completes just the metadata stage when the final object already exists.
var recoverFileMetadata func(reservation models.UploadReservation) (models.File, bool)

// SetFileMetadataRecovery registers the object-level recovery function
func SetFileMetadataRecovery(f func(reservation models.UploadReservation) (models.File, bool)) {
	recoverFileMetadata = f
}

// New atomically validates the current file request generation and persistently occupies
// one slot. The returned uuid is the upload / chunk identifier bound to this reservation.
func New(request models.FileRequest, apiKeyId string, maxSizeBytes int64) (string, error) {
	now := time.Now().Unix()
	for attempt := 0; attempt < 3; attempt++ {
		uuid := helper.GenerateRandomString(32)
		if _, exists := database.GetUploadReservation(uuid); exists {
			continue
		}
		reservation := models.UploadReservation{
			Uuid:          uuid,
			FileRequestId: request.Id,
			Version:       request.Version,
			ApiKeyId:      apiKeyId,
			MaxSizeBytes:  maxSizeBytes,
			Status:        models.ReservationStatusReserved,
			Expiry:        now + timeReservationWithoutUpload,
			Created:       now,
			Updated:       now,
		}
		startCleanup()
		if database.TryCreateUploadReservation(reservation, request.MaxFiles) {
			return uuid, nil
		}
		return "", ErrNoSlotLeft
	}
	return "", errors.New("could not create unique upload id")
}

// Get returns the persisted reservation for the given upload id
func Get(uuid string) (models.UploadReservation, bool) {
	return database.GetUploadReservation(uuid)
}

// GetCount returns the number of reservations that still occupy a slot for the request.
// The value is read from the persistent store and therefore shared across all instances.
func GetCount(requestId string) int {
	return database.CountActiveUploadReservations(requestId)
}

// MarkUploading validates the reservation against the current file request generation,
// extends the deadline to the upload deadline and records the maximum allowed file size.
// Chunks can only be written if this succeeds.
func MarkUploading(requestId, uuid, apiKeyId string) (models.UploadReservation, error) {
	reservation, err := getAndValidate(uuid, requestId, apiKeyId)
	if err != nil {
		return models.UploadReservation{}, err
	}
	if reservation.Status != models.ReservationStatusReserved &&
		reservation.Status != models.ReservationStatusUploading {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	now := time.Now().Unix()
	if !database.CompareAndSetReservationStatus(uuid,
		[]int{models.ReservationStatusReserved, models.ReservationStatusUploading},
		models.ReservationStatusUploading, now+timeReservationWithUpload, "") {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	reservation.Status = models.ReservationStatusUploading
	reservation.Expiry = now + timeReservationWithUpload
	return reservation, nil
}

// Claim atomically transitions the reservation to the finishing status and assigns the
// resulting file id, which is stored before the metadata exists so retries reuse it and
// never create a second file. The submitted file information is stored as a snapshot for
// reconciliation if a later commit response is uncertain.
func Claim(requestId, uuid, apiKeyId string, fileHeader chunking.FileHeader) (models.UploadReservation, error) {
	reservation, err := getAndValidate(uuid, requestId, apiKeyId)
	if err != nil {
		return models.UploadReservation{}, err
	}
	switch reservation.Status {
	case models.ReservationStatusCompleted:
		return reservation, ErrAlreadyCompleted
	case models.ReservationStatusFinishing:
		return reservation, ErrAlreadyProcessing
	case models.ReservationStatusCancelled:
		return models.UploadReservation{}, ErrReservationInvalid
	}
	if reservation.Status != models.ReservationStatusReserved &&
		reservation.Status != models.ReservationStatusUploading {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	fileId := helper.GenerateRandomString(environment.New().LengthId)
	now := time.Now().Unix()
	reservation.Status = models.ReservationStatusFinishing
	reservation.Updated = now
	reservation.FileId = fileId
	reservation.FileName = fileHeader.Filename
	reservation.ContentType = fileHeader.ContentType
	reservation.FileSize = fileHeader.Size
	// unconditional save carries the file snapshot; the preceding validation and CAS-like
	// status check ensure only the legitimate claimer reaches this point
	if !database.CompareAndSetReservationStatus(uuid,
		[]int{models.ReservationStatusReserved, models.ReservationStatusUploading},
		models.ReservationStatusFinishing, reservation.Expiry, fileId) {
		current, ok := database.GetUploadReservation(uuid)
		if ok && current.Status == models.ReservationStatusCompleted {
			return current, ErrAlreadyCompleted
		}
		return models.UploadReservation{}, ErrAlreadyProcessing
	}
	database.SaveUploadReservation(reservation)
	return reservation, nil
}

// RecordHash stores the hash of the finalised chunk before the object is moved to the
// filesystem. It is only recorded while the reservation is being finished, so the
// metadata stage can be recovered if the storage or database response is uncertain.
func RecordHash(uuid, sha1 string) {
	reservation, ok := database.GetUploadReservation(uuid)
	if !ok || reservation.Status != models.ReservationStatusFinishing {
		return
	}
	reservation.Sha1 = sha1
	database.SaveUploadReservation(reservation)
}

// ResumeForRetry moves a reservation back from finishing to uploading when the final
// object and metadata do not exist yet, so the client can retry the completion
func ResumeForRetry(uuid string) bool {
	if _, ok := database.GetUploadReservation(uuid); !ok {
		return false
	}
	return database.CompareAndSetReservationStatus(uuid,
		[]int{models.ReservationStatusFinishing},
		models.ReservationStatusUploading,
		time.Now().Unix()+timeReservationWithUpload, "")
}

// MarkCompleted converts the slot to a used one exactly once, after the file metadata
// was confirmed. Completed reservations are retained for idempotent retries.
func MarkCompleted(reservation models.UploadReservation) bool {
	return database.CompareAndSetReservationStatus(reservation.Uuid,
		[]int{models.ReservationStatusFinishing},
		models.ReservationStatusCompleted,
		time.Now().Unix()+timeReservationCompleted, reservation.FileId)
}

// Cancel releases the reservation (and its slot) and optionally removes the chunk file.
// It is idempotent and safe to call on already released reservations.
// Server-side failure releases pass an empty apiKeyId, submissions must pass their key.
func Cancel(requestId, uuid, apiKeyId string, removeChunk bool) bool {
	reservation, ok := database.GetUploadReservation(uuid)
	if !ok {
		if removeChunk {
			_ = chunking.DeleteChunk(uuid)
		}
		return true
	}
	if reservation.FileRequestId != requestId {
		return false
	}
	if apiKeyId != "" && reservation.ApiKeyId != apiKeyId {
		return false
	}
	if removeChunk {
		_ = chunking.DeleteChunk(uuid)
	}
	database.DeleteUploadReservation(uuid)
	return true
}

// CancelAllForRequest releases every reservation of a file request and its chunk files.
// It is used when the request is deleted.
func CancelAllForRequest(requestId string) {
	for _, reservation := range database.GetAllUploadReservations() {
		if reservation.FileRequestId != requestId {
			continue
		}
		_ = chunking.DeleteChunk(reservation.Uuid)
		database.DeleteUploadReservation(reservation.Uuid)
	}
}

// getAndValidate loads the reservation and checks that it still matches the current
// file request generation, API key and deadline
func getAndValidate(uuid, requestId, apiKeyId string) (models.UploadReservation, error) {
	reservation, ok := database.GetUploadReservation(uuid)
	if !ok {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	if reservation.FileRequestId != requestId {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	if apiKeyId != "" && reservation.ApiKeyId != apiKeyId {
		return models.UploadReservation{}, ErrReservationKey
	}
	if reservation.Expiry < time.Now().Unix() {
		return models.UploadReservation{}, ErrReservationExpired
	}
	request, ok := database.GetFileRequest(requestId)
	if !ok {
		return models.UploadReservation{}, ErrReservationInvalid
	}
	if request.Version != reservation.Version {
		return models.UploadReservation{}, ErrRequestVersion
	}
	if request.ApiKey != reservation.ApiKeyId {
		return models.UploadReservation{}, ErrRequestVersion
	}
	if !request.IsUnlimitedTime() && request.Expiry < time.Now().Unix() {
		return models.UploadReservation{}, ErrRequestExpired
	}
	return reservation, nil
}

// Cleanup releases expired and invalid reservations and reconciles reservations that
// got stuck while finishing, e.g. after a crash or an uncertain storage/database response.
// Confirmed file metadata is never deleted; only temporary chunk files are removed.
func Cleanup() {
	now := time.Now().Unix()
	for _, reservation := range database.GetAllUploadReservations() {
		switch reservation.Status {
		case models.ReservationStatusCompleted, models.ReservationStatusCancelled:
			if reservation.Expiry < now {
				database.DeleteUploadReservation(reservation.Uuid)
			}
		case models.ReservationStatusFinishing:
			reconcileFinishing(reservation, now)
		default:
			releaseActiveIfInvalidOrExpired(reservation, now)
		}
	}
}

// releaseActiveIfInvalidOrExpired releases reservations whose deadline passed or whose
// request generation is no longer current
func releaseActiveIfInvalidOrExpired(r models.UploadReservation, now int64) {
	if r.Expiry >= now {
		if _, err := getAndValidate(r.Uuid, r.FileRequestId, r.ApiKeyId); err == nil {
			return
		}
	}
	_ = chunking.DeleteChunk(r.Uuid)
	database.DeleteUploadReservation(r.Uuid)
}

// reconcileFinishing resumes or releases reservations stuck in the finishing state.
// If the file metadata was already confirmed, the reservation is marked completed;
// if the chunk still exists and the request is current, it is reopened for a retry,
// otherwise the slot is released.
func reconcileFinishing(r models.UploadReservation, now int64) {
	if now-r.Updated < finishingGrace && r.Expiry >= now {
		return
	}
	if r.FileId != "" {
		if _, ok := database.GetMetaDataById(r.FileId); ok {
			database.CompareAndSetReservationStatus(r.Uuid,
				[]int{models.ReservationStatusFinishing},
				models.ReservationStatusCompleted, now+timeReservationCompleted, r.FileId)
			return
		}
	}
	_, valid := getAndValidate(r.Uuid, r.FileRequestId, r.ApiKeyId)
	if valid == nil && chunking.FileExists(r.Uuid) {
		if database.CompareAndSetReservationStatus(r.Uuid,
			[]int{models.ReservationStatusFinishing},
			models.ReservationStatusUploading, now+timeReservationWithUpload, "") {
			return
		}
	}
	// The chunk is gone: if the final object exists, complete only the missing metadata
	// stage; otherwise release the slot. Confirmed files are never deleted here.
	if valid == nil && r.Sha1 != "" && recoverFileMetadata != nil {
		if file, recovered := recoverFileMetadata(r); recovered {
			database.CompareAndSetReservationStatus(r.Uuid,
				[]int{models.ReservationStatusFinishing},
				models.ReservationStatusCompleted, now+timeReservationCompleted, file.Id)
			return
		}
	}
	_ = chunking.DeleteChunk(r.Uuid)
	database.DeleteUploadReservation(r.Uuid)
}

// startCleanup runs the periodic cleanup once per process
func startCleanup() {
	startCleanupOnce.Do(func() {
		go func() {
			for {
				time.Sleep(cleanupEvery)
				Cleanup()
			}
		}()
	})
}
