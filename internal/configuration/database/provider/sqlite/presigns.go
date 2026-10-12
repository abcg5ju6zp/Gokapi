package sqlite

import (
	"database/sql"
	"errors"

	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
)

type schemaPresigns struct {
	Id         string
	UserId     int
	Files      string
	Filename   string
	Expiry     int64
	Status     int
	ClaimedAt  int64
	ClaimToken string
}

func scanPresign(row interface {
	Scan(dest ...any) error
}) (models.Presign, error) {
	var rowResult schemaPresigns
	err := row.Scan(&rowResult.Id, &rowResult.UserId, &rowResult.Files, &rowResult.Filename, &rowResult.Expiry,
		&rowResult.Status, &rowResult.ClaimedAt, &rowResult.ClaimToken)
	if err != nil {
		return models.Presign{}, err
	}
	result := models.Presign{
		Id:         rowResult.Id,
		UserId:     rowResult.UserId,
		FilesJson:  rowResult.Files,
		Filename:   rowResult.Filename,
		Expiry:     rowResult.Expiry,
		Status:     rowResult.Status,
		ClaimedAt:  rowResult.ClaimedAt,
		ClaimToken: rowResult.ClaimToken,
	}
	err = result.UnmarshalFiles()
	return result, err
}

// GetPresign returns the presigned download credential with the given ID
func (p DatabaseProvider) GetPresign(id string) (models.Presign, bool) {
	row := p.sqliteDb.QueryRow("SELECT Id, UserId, Files, Filename, Expiry, Status, ClaimedAt, ClaimToken FROM Presigns WHERE Id = ?", id)
	result, err := scanPresign(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Presign{}, false
		}
		helper.Check(err)
		return models.Presign{}, false
	}
	return result, true
}

// GetAllPresigns returns all persisted presigned download credentials
func (p DatabaseProvider) GetAllPresigns() []models.Presign {
	result := make([]models.Presign, 0)
	rows, err := p.sqliteDb.Query("SELECT Id, UserId, Files, Filename, Expiry, Status, ClaimedAt, ClaimToken FROM Presigns")
	if err != nil {
		helper.Check(err)
		return result
	}
	defer rows.Close()
	for rows.Next() {
		presign, err := scanPresign(rows)
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
	_, err = p.sqliteDb.Exec(`INSERT OR REPLACE INTO Presigns (Id, UserId, Files, Filename, Expiry, Status, ClaimedAt, ClaimToken)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		presign.Id, presign.UserId, presign.FilesJson, presign.Filename, presign.Expiry,
		presign.Status, presign.ClaimedAt, presign.ClaimToken)
	helper.Check(err)
}

// DeletePresign deletes a presigned download credential with the given ID
func (p DatabaseProvider) DeletePresign(id string) {
	if id == "" {
		return
	}
	_, err := p.sqliteDb.Exec("DELETE FROM Presigns WHERE Id = ?", id)
	helper.Check(err)
}

// ClaimPresign atomically transitions an available, unexpired credential to the claimed state
func (p DatabaseProvider) ClaimPresign(id string, now int64, token string) (models.Presign, bool) {
	result, err := p.sqliteDb.Exec(`UPDATE Presigns SET Status = ?, ClaimedAt = ?, ClaimToken = ?
		WHERE Id = ? AND Status = ? AND Expiry >= ?`,
		models.PresignClaimed, now, token, id, models.PresignAvailable, now)
	if err != nil {
		helper.Check(err)
		return models.Presign{}, false
	}
	rowsChanged, err := result.RowsAffected()
	if err != nil {
		helper.Check(err)
		return models.Presign{}, false
	}
	if rowsChanged != 1 {
		return models.Presign{}, false
	}
	return p.GetPresign(id)
}

// ReleasePresign atomically returns a claimed credential to the available state
func (p DatabaseProvider) ReleasePresign(id string, token string) bool {
	result, err := p.sqliteDb.Exec(`UPDATE Presigns SET Status = ?, ClaimedAt = 0, ClaimToken = ''
		WHERE Id = ? AND Status = ? AND ClaimToken = ?`,
		models.PresignAvailable, id, models.PresignClaimed, token)
	if err != nil {
		helper.Check(err)
		return false
	}
	rowsChanged, err := result.RowsAffected()
	helper.Check(err)
	return rowsChanged == 1
}

// SetPresignDelivered atomically transitions a claimed credential to the terminal delivered state
func (p DatabaseProvider) SetPresignDelivered(id string, token string) bool {
	result, err := p.sqliteDb.Exec(`UPDATE Presigns SET Status = ?
		WHERE Id = ? AND Status = ? AND ClaimToken = ?`,
		models.PresignDelivered, id, models.PresignClaimed, token)
	if err != nil {
		helper.Check(err)
		return false
	}
	rowsChanged, err := result.RowsAffected()
	helper.Check(err)
	return rowsChanged == 1
}
