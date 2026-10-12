// Package presign manages one-time, time-limited download credentials.
//
// A credential is persisted in the database and binds the immutable
// authorization snapshot taken at issuance (authorized file set, file
// versions, archive name and validity period). Consumption is a state machine
// with atomic transitions so that exactly one consumer can progress across
// instances, restarts and cleanup runs:
//
//	available -> claimed -> delivered (terminal)
//	             claimed -> available (release on pre-commit failure)
package presign

import (
	"time"

	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
)

const (
	// defaultValidity is the lifetime of a newly issued credential
	defaultValidity = 30 * time.Second
	// claimTimeout bounds the pre-commit phase of a claim. A claim older than
	// this belongs to an instance that died or stalled before committing the
	// response, and recovery releases or deletes it. Normal pre-commit work
	// (opening local files, building an S3 link) completes within seconds.
	claimTimeout = 2 * time.Minute
	// tokenLength is the length of the per-claim fencing token
	tokenLength = 30
)

// New creates a new credential from the authorized file snapshot. It is not
// persisted yet; call Save after creation.
func New(files []models.File, user models.User, archiveName string) models.Presign {
	snapshot := make([]models.PresignFile, 0, len(files))
	for _, file := range files {
		snapshot = append(snapshot, models.PresignFile{
			Id:     file.Id,
			SHA1:   file.SHA1,
			UserId: file.UserId,
		})
	}
	return models.Presign{
		Id:       helper.GenerateRandomString(60),
		UserId:   user.Id,
		Files:    snapshot,
		Filename: archiveName,
		Expiry:   time.Now().Add(defaultValidity).Unix(),
		Status:   models.PresignAvailable,
	}
}

// Save persists a credential. The download URL must only be returned after
// this call succeeds, so a rolling restart cannot lose an unused credential.
func Save(presign models.Presign) {
	database.SavePresign(presign)
}

// Get returns a credential by ID if it exists and has not expired, regardless
// of its current state.
func Get(id string) (models.Presign, bool) {
	if id == "" {
		return models.Presign{}, false
	}
	result, ok := database.GetPresign(id)
	if !ok {
		return models.Presign{}, false
	}
	if result.Expiry < time.Now().Unix() {
		return models.Presign{}, false
	}
	return result, true
}

// TryClaim atomically checks out an available, unexpired credential. Only one
// concurrent caller receives the credential; all others get false. The
// returned credential carries a fencing token that is required for Release
// and MarkDelivered, so a late owner can no longer change state after a
// timeout recovery moved the generation along.
func TryClaim(id string) (models.Presign, bool) {
	token := helper.GenerateRandomString(tokenLength)
	return database.ClaimPresign(id, time.Now().Unix(), token)
}

// Release returns a claim to the available state after a failure before the
// response was committed, making the same authorization snapshot retryable.
func Release(presign models.Presign) bool {
	return database.ReleasePresign(presign.Id, presign.ClaimToken)
}

// MarkDelivered records the terminal state once the body delivery has started
// or the remote redirect has been committed.
func MarkDelivered(presign models.Presign) bool {
	return database.SetPresignDelivered(presign.Id, presign.ClaimToken)
}

// Delete removes a credential
func Delete(id string) {
	database.DeletePresign(id)
}

// RecoverAndClean is invoked at startup and during periodic cleanup. It uses
// the delivery checkpoint (claim state) to reconcile credentials left behind
// by crashed or restarted instances:
//   - expired credentials in any state are deleted,
//   - claims whose holder died before the delivery checkpoint are released if
//     the credential is still valid, so the credential stays retryable,
//   - delivered credentials are terminal and are deleted once expired.
//
// All transitions are conditional database updates, so concurrent cleanup
// runs interleaving with live claims can never move a generation that another
// consumer is actively progressing.
func RecoverAndClean() {
	now := time.Now().Unix()
	for _, presign := range database.GetAllPresigns() {
		if presign.Expiry < now {
			database.DeletePresign(presign.Id)
			continue
		}
		if presign.Status == models.PresignClaimed &&
			now-presign.ClaimedAt > int64(claimTimeout/time.Second) {
			database.ReleasePresign(presign.Id, presign.ClaimToken)
		}
	}
}
