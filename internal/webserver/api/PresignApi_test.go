//go:build test

package api

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/test"
)

func TestCreatePresignedUrlPersistsBeforeResponse(t *testing.T) {
	files := []models.File{
		{Id: "ps-api-1", Name: "a.txt", SHA1: "ps-api-sha1", UserId: idUser},
		{Id: "ps-api-2", Name: "b.txt", SHA1: "ps-api-sha2", UserId: idUser},
	}
	recorder := httptest.NewRecorder()
	createAndOutputPresignedUrl(files, models.User{Id: idUser}, recorder, "api-archive")

	var response struct {
		Result      string `json:"Result"`
		DownloadUrl string `json:"downloadUrl"`
	}
	err := json.Unmarshal(recorder.Body.Bytes(), &response)
	test.IsNil(t, err)
	test.IsEqualString(t, response.Result, "OK")

	parsed, err := url.Parse(response.DownloadUrl)
	test.IsNil(t, err)
	key := parsed.Query().Get("key")
	test.IsNotEmpty(t, key)

	// The credential must already be persisted when the URL is returned, so a
	// rolling restart cannot lose an unused address.
	credential, ok := database.GetPresign(key)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, credential.Status, models.PresignAvailable)
	test.IsEqualString(t, credential.Filename, "api-archive")
	test.IsEqualInt(t, credential.UserId, idUser)
	test.IsEqualInt(t, len(credential.Files), 2)
	test.IsEqualString(t, credential.Files[0].Id, "ps-api-1")
	test.IsEqualString(t, credential.Files[0].SHA1, "ps-api-sha1")
	test.IsEqualString(t, credential.Files[1].Id, "ps-api-2")
	test.IsEqualBool(t, credential.Expiry > time.Now().Unix(), true)
}
