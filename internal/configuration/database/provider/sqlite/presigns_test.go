//go:build test

package sqlite

import (
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/test"
)

func TestDatabaseProvider_PresignsUpgrade(t *testing.T) {
	instance, err := New(models.DbConnection{
		HostUrl: "./test/newfolder/gokapi_presign_upgrade.sqlite",
		Type:    0,
	})
	test.IsNil(t, err)
	defer instance.Close()

	previousVersion := instance.GetDbVersion()
	defer instance.SetDbVersion(previousVersion)

	// Simulate a v15 database that predates the Presigns table.
	_, err = instance.sqliteDb.Exec("DROP TABLE Presigns")
	test.IsNil(t, err)
	instance.SetDbVersion(15)

	instance.Upgrade(15)

	credential := models.Presign{
		Id:       "upgradecred",
		UserId:   5,
		Files:    []models.PresignFile{{Id: "f1", SHA1: "sha1", UserId: 5}},
		Filename: "archive",
		Expiry:   time.Now().Add(time.Minute).Unix(),
		Status:   models.PresignAvailable,
	}
	instance.SavePresign(credential)
	loaded, ok := instance.GetPresign("upgradecred")
	test.IsEqualBool(t, ok, true)
	test.IsEqualString(t, loaded.Filename, "archive")
	test.IsEqualInt(t, len(loaded.Files), 1)
	test.IsEqualString(t, loaded.Files[0].SHA1, "sha1")

	// Atomic claim and terminal transitions work on the upgraded schema.
	claimed, ok := instance.ClaimPresign("upgradecred", time.Now().Unix(), "token1")
	test.IsEqualBool(t, ok, true)
	test.IsEqualString(t, claimed.ClaimToken, "token1")
	_, ok = instance.ClaimPresign("upgradecred", time.Now().Unix(), "token2")
	test.IsEqualBool(t, ok, false)
	test.IsEqualBool(t, instance.ReleasePresign("upgradecred", "token1"), true)
	claimed, ok = instance.ClaimPresign("upgradecred", time.Now().Unix(), "token3")
	test.IsEqualBool(t, ok, true)
	test.IsEqualBool(t, instance.SetPresignDelivered("upgradecred", "token3"), true)
	test.IsEqualBool(t, instance.ReleasePresign("upgradecred", "token3"), false)
	instance.DeletePresign("upgradecred")
	_, ok = instance.GetPresign("upgradecred")
	test.IsEqualBool(t, ok, false)
}
