package sqlite

import (
	"context"
	"strings"
	"time"

	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
)

// reservationColumnOrder is the column order of the UploadReservations table,
// it must be kept in sync when adding new columns
var reservationColumnOrder = []string{
	"Uuid", "FileRequestId", "Version", "ApiKeyId", "MaxSizeBytes",
	"Status", "Expiry", "Created", "Updated", "FileId",
	"FileName", "ContentType", "FileSize", "Sha1",
}

// reservationColumns lists the writeable columns in insert order
var reservationColumns = reservationColumnOrder

func scanReservation(scanner interface {
	Scan(dest ...any) error
}) (models.UploadReservation, error) {
	var r models.UploadReservation
	err := scanner.Scan(&r.Uuid, &r.FileRequestId, &r.Version, &r.ApiKeyId, &r.MaxSizeBytes,
		&r.Status, &r.Expiry, &r.Created, &r.Updated, &r.FileId,
		&r.FileName, &r.ContentType, &r.FileSize, &r.Sha1)
	return r, err
}

func reservationValues(r models.UploadReservation) []any {
	return []any{
		r.Uuid, r.FileRequestId, r.Version, r.ApiKeyId, r.MaxSizeBytes,
		r.Status, r.Expiry, r.Created, r.Updated, r.FileId,
		r.FileName, r.ContentType, r.FileSize, r.Sha1,
	}
}

// GetUploadReservation returns the reservation for the given upload id
func (p DatabaseProvider) GetUploadReservation(uuid string) (models.UploadReservation, bool) {
	if uuid == "" {
		return models.UploadReservation{}, false
	}
	row := p.sqliteDb.QueryRow("SELECT "+strings.Join(reservationColumnOrder, ", ")+
		" FROM UploadReservations WHERE Uuid = ?", uuid)
	r, err := scanReservation(row)
	if err != nil {
		return models.UploadReservation{}, false
	}
	return r, true
}

// GetAllUploadReservations returns all stored upload reservations
func (p DatabaseProvider) GetAllUploadReservations() []models.UploadReservation {
	result := make([]models.UploadReservation, 0)
	rows, err := p.sqliteDb.Query("SELECT " + strings.Join(reservationColumnOrder, ", ") + " FROM UploadReservations")
	helper.Check(err)
	defer rows.Close()
	for rows.Next() {
		r, err := scanReservation(rows)
		helper.Check(err)
		result = append(result, r)
	}
	return result
}

// CountActiveUploadReservations returns the number of reservations that still occupy a slot
func (p DatabaseProvider) CountActiveUploadReservations(fileRequestId string) int {
	var count int
	row := p.sqliteDb.QueryRow(
		"SELECT COUNT(*) FROM UploadReservations WHERE FileRequestId = ? AND Status <= ?",
		fileRequestId, models.ReservationStatusFinishing)
	err := row.Scan(&count)
	helper.Check(err)
	return count
}

// TryCreateUploadReservation atomically occupies a slot if the request still has room.
// BEGIN IMMEDIATE serialises writers, so concurrent instances cannot oversubscribe.
func (p DatabaseProvider) TryCreateUploadReservation(reservation models.UploadReservation, maxFiles int) bool {
	ctx := context.Background()
	conn, err := p.sqliteDb.Conn(ctx)
	if err != nil {
		helper.Check(err)
		return false
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
	if err != nil {
		helper.Check(err)
		return false
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	// Validate the current request generation inside the write transaction, so a
	// concurrent edit, key rotation, expiry or deletion cannot oversubscribe a slot
	var currentVersion int64
	var currentApiKey string
	var currentExpiry int64
	err = conn.QueryRowContext(ctx,
		"SELECT Version, ApiKey, Expiry FROM UploadRequests WHERE Id = ?",
		reservation.FileRequestId).Scan(&currentVersion, &currentApiKey, &currentExpiry)
	if err != nil {
		return false
	}
	if currentVersion != reservation.Version || currentApiKey != reservation.ApiKeyId {
		return false
	}
	if currentExpiry != 0 && currentExpiry < reservation.Created {
		return false
	}

	if maxFiles > 0 {
		var uploadedFiles int
		err = conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM FileMetaData WHERE UploadRequestId = ? AND PendingDeletion = 0",
			reservation.FileRequestId).Scan(&uploadedFiles)
		if err != nil {
			helper.Check(err)
			return false
		}
		var activeReservations int
		err = conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM UploadReservations WHERE FileRequestId = ? AND Status <= ?",
			reservation.FileRequestId, models.ReservationStatusFinishing).Scan(&activeReservations)
		if err != nil {
			helper.Check(err)
			return false
		}
		if uploadedFiles+activeReservations >= maxFiles {
			return false
		}
	}

	_, err = conn.ExecContext(ctx,
		`INSERT INTO UploadReservations (`+strings.Join(reservationColumns, ", ")+`)
		 VALUES (`+strings.Repeat("?,", len(reservationColumns)-1)+`?)`,
		reservationValues(reservation)...)
	if err != nil {
		// e.g. a concurrent reservation with the same uuid won a race; treat as failed claim
		return false
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		helper.Check(err)
		return false
	}
	committed = true
	return true
}

// SaveUploadReservation stores the reservation unconditionally (used for recovery)
func (p DatabaseProvider) SaveUploadReservation(r models.UploadReservation) {
	_, err := p.sqliteDb.Exec(
		`INSERT OR REPLACE INTO UploadReservations (`+strings.Join(reservationColumns, ", ")+`)
		 VALUES (`+strings.Repeat("?,", len(reservationColumns)-1)+`?)`,
		reservationValues(r)...)
	helper.Check(err)
}

// CompareAndSetReservationStatus atomically transitions the status if the current one is allowed
func (p DatabaseProvider) CompareAndSetReservationStatus(uuid string, allowedFromStatuses []int, newStatus int, newExpiry int64, fileId string) bool {
	if len(allowedFromStatuses) == 0 {
		return false
	}
	placeholders := strings.Repeat("?,", len(allowedFromStatuses))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, 0, len(allowedFromStatuses)+4)
	setClause := "Status = ?, Expiry = ?, Updated = ?"
	args = append(args, newStatus, newExpiry, time.Now().Unix())
	if fileId != "" {
		setClause += ", FileId = ?"
		args = append(args, fileId)
	}
	args = append(args, uuid)
	for _, status := range allowedFromStatuses {
		args = append(args, status)
	}
	query := "UPDATE UploadReservations SET " + setClause + " WHERE Uuid = ? AND Status IN (" + placeholders + ")"
	result, err := p.sqliteDb.Exec(query, args...)
	if err != nil {
		helper.Check(err)
		return false
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		helper.Check(err)
		return false
	}
	return rowsAffected == 1
}

// DeleteUploadReservation deletes the reservation with the given uuid
func (p DatabaseProvider) DeleteUploadReservation(uuid string) {
	_, err := p.sqliteDb.Exec("DELETE FROM UploadReservations WHERE Uuid = ?", uuid)
	helper.Check(err)
}

// DeleteUploadReservationsForRequest deletes all reservations of the given file request
func (p DatabaseProvider) DeleteUploadReservationsForRequest(fileRequestId string) {
	_, err := p.sqliteDb.Exec("DELETE FROM UploadReservations WHERE FileRequestId = ?", fileRequestId)
	helper.Check(err)
}
