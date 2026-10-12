package models

// Reservation lifecycle:
//
//	reserved -> uploading -> finishing -> completed
//	                     \-> cancelled (terminal)
//
// Reservations are persisted when the upload id is requested, so all instances
// share the same remaining slot count and reservations survive a restart.
const (
	// ReservationStatusReserved is set when an upload id is handed out, before the first chunk arrives
	ReservationStatusReserved = 0
	// ReservationStatusUploading is set once the first valid chunk has been written
	ReservationStatusUploading = 1
	// ReservationStatusFinishing is set atomically when a complete call claims the reservation
	ReservationStatusFinishing = 2
	// ReservationStatusCompleted is a terminal status set once the resulting file metadata was confirmed
	ReservationStatusCompleted = 3
	// ReservationStatusCancelled is a terminal status for explicit cancels and failed validation
	ReservationStatusCancelled = 4
)

// IsTerminal returns true if the reservation cannot be advanced anymore
func (r UploadReservation) IsTerminal() bool {
	return r.Status == ReservationStatusCompleted || r.Status == ReservationStatusCancelled
}

// IsActive returns true if the reservation still occupies a slot and is neither
// completed nor cancelled
func (r UploadReservation) IsActive() bool {
	return r.Status == ReservationStatusReserved ||
		r.Status == ReservationStatusUploading ||
		r.Status == ReservationStatusFinishing
}

// UploadReservation contains the persistent reservation of one file slot of a file request
type UploadReservation struct {
	// Uuid is the chunk / upload id the reservation was created for
	Uuid string `json:"uuid" redis:"uuid"`
	// FileRequestId is the id of the file request the slot belongs to
	FileRequestId string `json:"filerequestid" redis:"filerequestid"`
	// Version is the generation of the file request at reservation time.
	// It is increased whenever the request is edited or its API key is rotated,
	// so reservations of older generations can no longer write to the request.
	Version int64 `json:"version" redis:"version"`
	// ApiKeyId is the file request API key that created the reservation
	ApiKeyId string `json:"apikeyid" redis:"apikeyid"`
	// MaxSizeBytes is the maximum allowed total file size at reservation time.
	// 0 means that only the server-wide limit applies.
	MaxSizeBytes int64 `json:"maxsizebytes" redis:"maxsizebytes"`
	// Status is one of the ReservationStatus* constants
	Status int `json:"status" redis:"status"`
	// Expiry is the unix timestamp at which the reservation is released automatically
	Expiry int64 `json:"expiry" redis:"expiry"`
	// Created is the unix timestamp of the reservation creation
	Created int64 `json:"created" redis:"created"`
	// Updated is the unix timestamp of the last status transition
	Updated int64 `json:"updated" redis:"updated"`
	// FileId is assigned when the completion is claimed and retained on the completed
	// reservation, allowing idempotent retries without creating a second file.
	FileId string `json:"fileid" redis:"fileid"`
	// FileName is the submitted name of the uploaded file, stored at claim time for recovery
	FileName string `json:"filename" redis:"filename"`
	// ContentType is the submitted content type of the uploaded file
	ContentType string `json:"contenttype" redis:"contenttype"`
	// FileSize is the submitted total size of the uploaded file
	FileSize int64 `json:"filesize" redis:"filesize"`
	// Sha1 is the hash of the finalised chunk, recorded before the object is moved to
	// the filesystem. It allows completing the metadata if the commit response was lost.
	Sha1 string `json:"sha1" redis:"sha1"`
}
