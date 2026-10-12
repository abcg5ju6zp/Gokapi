//go:build !integration && test

package webserver

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/configuration"
	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/storage/presign"
	"github.com/forceu/gokapi/internal/test"
)

func createPresignTestFile(t *testing.T, id, content string) models.File {
	sha := "presignsha-" + helper.GenerateRandomString(10)
	err := os.WriteFile(configuration.Get().DataDir+"/"+sha, []byte(content), 0644)
	test.IsNil(t, err)
	file := models.File{
		Id:                 id,
		Name:               id + ".txt",
		Size:               fmt.Sprintf("%d B", len(content)),
		SizeBytes:          int64(len(content)),
		SHA1:               sha,
		ExpireAt:           2147483646,
		ContentType:        "text/plain",
		UnlimitedDownloads: true,
		UnlimitedTime:      true,
		UserId:             5,
	}
	database.SaveMetaData(file)
	return file
}

func issueTestPresign(files []models.File, filename string) models.Presign {
	credential := presign.New(files, models.User{Id: 5}, filename)
	presign.Save(credential)
	return credential
}

func callDownloadPresigned(key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/downloadPresigned?key="+key, nil)
	w := httptest.NewRecorder()
	downloadPresigned(w, r)
	return w
}

func TestPresignedSingleFileOneTimeDelivery(t *testing.T) {
	file := createPresignTestFile(t, "ps-single-"+helper.GenerateRandomString(6), "presign-body-content")
	credential := issueTestPresign([]models.File{file}, "")

	w := callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusOK)
	body, err := io.ReadAll(w.Result().Body)
	test.IsNil(t, err)
	test.IsEqualString(t, string(body), "presign-body-content")
	test.IsEqualBool(t, w.Result().Header.Get("Content-Disposition") != "", true)

	// Terminal: a second request (lost response, retry, late client) cannot deliver again.
	w = callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
}

func TestPresignedZipDelivery(t *testing.T) {
	file1 := createPresignTestFile(t, "ps-zip1-"+helper.GenerateRandomString(6), "content-one")
	file2 := createPresignTestFile(t, "ps-zip2-"+helper.GenerateRandomString(6), "content-two")
	credential := issueTestPresign([]models.File{file1, file2}, "my-bundle")

	w := callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusOK)
	test.IsEqualString(t, w.Result().Header.Get("Content-Type"), "application/zip")
	test.IsEqualBool(t, len(w.Result().Header.Get("Content-Disposition")) > 0, true)
	body, _ := io.ReadAll(w.Result().Body)
	test.IsEqualBool(t, len(body) >= 4, true)
	test.IsEqualString(t, string(body[0:2]), "PK")

	w = callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
}

func TestPresignedReleaseBeforeCommitIsRetryable(t *testing.T) {
	file := createPresignTestFile(t, "ps-retry-"+helper.GenerateRandomString(6), "retryable-body")
	credential := issueTestPresign([]models.File{file}, "")

	// The snapshot validation passes (the file still exists), but opening the
	// local source fails before any response header is committed.
	dataPath := configuration.Get().DataDir + "/" + file.SHA1
	err := os.Chmod(dataPath, 0)
	test.IsNil(t, err)
	defer os.Chmod(dataPath, 0644)

	w := callDownloadPresigned(credential.Id)
	if w.Code == http.StatusOK {
		// Privileged processes (root) bypass file permissions - the release
		// precondition cannot be exercised in that environment.
		t.Skip("pre-commit open failure cannot be simulated for privileged user")
	}
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)

	// The claim was released, the credential is still available...
	stored, ok := database.GetPresign(credential.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, stored.Status, models.PresignAvailable)

	// ...and after the problem is fixed, the same link delivers the same snapshot.
	err = os.Chmod(dataPath, 0644)
	test.IsNil(t, err)

	w = callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusOK)
	body, err := io.ReadAll(w.Result().Body)
	test.IsNil(t, err)
	test.IsEqualString(t, string(body), "retryable-body")

	// Delivered, so no third delivery.
	w = callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
}

func TestPresignedSnapshotRevalidationBurnsCredential(t *testing.T) {
	file := createPresignTestFile(t, "ps-snap-"+helper.GenerateRandomString(6), "snapshot-body")
	credential := issueTestPresign([]models.File{file}, "")

	// Content version replacement: an old link must not read new content.
	current, ok := database.GetMetaDataById(file.Id)
	test.IsEqualBool(t, ok, true)
	current.SHA1 = "presignsha-replaced-" + helper.GenerateRandomString(6)
	database.SaveMetaData(current)
	w := callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
	stored, ok := database.GetPresign(credential.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, stored.Status, models.PresignDelivered)
	w = callDownloadPresigned(credential.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)

	// Ownership change after issuance burns the credential as well.
	file2 := createPresignTestFile(t, "ps-owner-"+helper.GenerateRandomString(6), "owner-body")
	credential2 := issueTestPresign([]models.File{file2}, "")
	current2, _ := database.GetMetaDataById(file2.Id)
	current2.UserId = 99
	database.SaveMetaData(current2)
	w = callDownloadPresigned(credential2.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
	w = callDownloadPresigned(credential2.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)

	// Expired / pending-deletion files cannot be fetched through an old link.
	file3 := createPresignTestFile(t, "ps-exp-"+helper.GenerateRandomString(6), "expiring-body")
	credential3 := issueTestPresign([]models.File{file3}, "")
	current3, _ := database.GetMetaDataById(file3.Id)
	current3.UnlimitedTime = false
	current3.ExpireAt = time.Now().Add(-time.Minute).Unix()
	database.SaveMetaData(current3)
	w = callDownloadPresigned(credential3.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
	w = callDownloadPresigned(credential3.Id)
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)
}

func TestPresignedConcurrentDownloadSingleWinner(t *testing.T) {
	file := createPresignTestFile(t, "ps-conc-"+helper.GenerateRandomString(6), "concurrent-body")
	credential := issueTestPresign([]models.File{file}, "")

	const workers = 50
	codes := make(chan int, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			codes <- callDownloadPresigned(credential.Id).Code
		}()
	}
	wg.Wait()
	close(codes)
	okCount, badCount := 0, 0
	for code := range codes {
		if code == http.StatusOK {
			okCount++
		} else {
			badCount++
		}
	}
	test.IsEqualInt(t, okCount, 1)
	test.IsEqualInt(t, badCount, workers-1)
}

func TestPresignedInvalidKey(t *testing.T) {
	w := callDownloadPresigned("nonexistent-key")
	test.IsEqualInt(t, w.Code, http.StatusBadRequest)

	r := httptest.NewRequest("GET", "/downloadPresigned", nil)
	w2 := httptest.NewRecorder()
	downloadPresigned(w2, r)
	test.IsEqualInt(t, w2.Code, http.StatusBadRequest)
}
