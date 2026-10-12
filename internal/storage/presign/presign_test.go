//go:build test

package presign

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/forceu/gokapi/internal/configuration"
	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/configuration/database/dbabstraction"
	"github.com/forceu/gokapi/internal/models"
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

func testFiles() []models.File {
	return []models.File{
		{Id: "presignfile1", Name: "a.txt", SHA1: "sha1-a", UserId: 5},
		{Id: "presignfile2", Name: "b.txt", SHA1: "sha1-b", UserId: 5},
	}
}

func credential(t *testing.T, files []models.File) models.Presign {
	user := models.User{Id: 5}
	p := New(files, user, "archive")
	test.IsEqualInt(t, p.UserId, 5)
	test.IsEqualString(t, p.Filename, "archive")
	test.IsEqualInt(t, p.Status, models.PresignAvailable)
	test.IsEqualBool(t, len(p.Files) == len(files), true)
	test.IsEqualString(t, p.Files[0].Id, files[0].Id)
	test.IsEqualString(t, p.Files[0].SHA1, files[0].SHA1)
	test.IsEqualInt(t, p.Files[0].UserId, files[0].UserId)
	test.IsEqualBool(t, p.Expiry > time.Now().Unix(), true)
	test.IsEqualBool(t, len(p.Id) == 60, true)
	Save(p)
	return p
}

func TestIssueSnapshotAndPersistence(t *testing.T) {
	p := credential(t, testFiles())
	loaded, ok := Get(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, len(loaded.Files), 2)
	test.IsEqualString(t, loaded.Files[1].SHA1, "sha1-b")
	test.IsEqualString(t, loaded.Filename, "archive")

	// A fresh database connection (simulating an instance restart after a
	// rolling update) must still see the unused credential.
	dbConfig, err := database.ParseUrl(testconfiguration.SqliteUrl, false)
	test.IsNil(t, err)
	restartDb, err := dbabstraction.GetNew(dbConfig)
	test.IsNil(t, err)
	reloaded, ok := restartDb.GetPresign(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, reloaded.Status, models.PresignAvailable)
	test.IsEqualInt(t, len(reloaded.Files), 2)
	test.IsEqualString(t, reloaded.Files[0].Id, "presignfile1")
	test.IsEqualBool(t, reloaded.FilesJson != "", true)
	restartDb.Close()
}

func TestClaimIsExclusive(t *testing.T) {
	p := credential(t, testFiles()[:1])
	claimed, ok := TryClaim(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, claimed.Status, models.PresignClaimed)
	test.IsNotEmpty(t, claimed.ClaimToken)

	// A second concurrent/sequential consumer cannot pass the claim.
	_, ok = TryClaim(p.Id)
	test.IsEqualBool(t, ok, false)

	// A non-existent id cannot be claimed.
	_, ok = TryClaim("doesnotexist")
	test.IsEqualBool(t, ok, false)
}

func TestConcurrentClaimHasSingleWinner(t *testing.T) {
	p := credential(t, testFiles()[:1])
	const workers = 50
	var winners int
	var winnerTokens []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			claimed, ok := TryClaim(p.Id)
			if ok {
				mu.Lock()
				winners++
				winnerTokens = append(winnerTokens, claimed.ClaimToken)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	test.IsEqualInt(t, winners, 1)
	test.IsEqualInt(t, len(winnerTokens), 1)
}

func TestReleaseAllowsRetrySameSnapshot(t *testing.T) {
	p := credential(t, testFiles())
	first, ok := TryClaim(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualBool(t, Release(first), true)

	// Retry reuses the same credential and file snapshot; new claim generation.
	second, ok := TryClaim(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualBool(t, second.ClaimToken == first.ClaimToken, false)
	test.IsEqualInt(t, len(second.Files), 2)

	// The stale generation can no longer release or finalize the claim.
	test.IsEqualBool(t, Release(first), false)
	test.IsEqualBool(t, MarkDelivered(first), false)

	// Delivering with the current generation is terminal.
	test.IsEqualBool(t, MarkDelivered(second), true)
	_, ok = TryClaim(p.Id)
	test.IsEqualBool(t, ok, false)
	test.IsEqualBool(t, Release(second), false)
}

func TestDeliveredIsTerminal(t *testing.T) {
	p := credential(t, testFiles()[:1])
	claimed, ok := TryClaim(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualBool(t, MarkDelivered(claimed), true)
	// Marking again is a no-op.
	test.IsEqualBool(t, MarkDelivered(claimed), false)
	_, ok = TryClaim(p.Id)
	test.IsEqualBool(t, ok, false)
	loaded, ok := Get(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, loaded.Status, models.PresignDelivered)
}

func TestExpiredCredentialCannotBeClaimed(t *testing.T) {
	p := credential(t, testFiles()[:1])
	p.Expiry = time.Now().Add(-time.Second).Unix()
	Save(p)
	_, ok := Get(p.Id)
	test.IsEqualBool(t, ok, false)
	_, ok = TryClaim(p.Id)
	test.IsEqualBool(t, ok, false)
}

func TestRecoverAndClean(t *testing.T) {
	now := time.Now().Unix()

	expiredAvailable := credential(t, []models.File{{Id: "expAvail", SHA1: "s", UserId: 5}})
	expiredAvailable.Expiry = now - 1
	Save(expiredAvailable)

	expiredDelivered := credential(t, []models.File{{Id: "expDel", SHA1: "s", UserId: 5}})
	expiredDelivered.Expiry = now - 1
	expiredDelivered.Status = models.PresignDelivered
	Save(expiredDelivered)

	staleClaimed := credential(t, []models.File{{Id: "stale", SHA1: "s", UserId: 5}})
	staleClaimed.Status = models.PresignClaimed
	staleClaimed.ClaimedAt = now - 200
	staleClaimed.ClaimToken = "oldtoken"
	Save(staleClaimed)

	activeClaim := credential(t, []models.File{{Id: "active", SHA1: "s", UserId: 5}})
	activeClaim.Status = models.PresignClaimed
	activeClaim.ClaimedAt = now
	activeClaim.ClaimToken = "newtoken"
	Save(activeClaim)

	activeDelivered := credential(t, []models.File{{Id: "delivered", SHA1: "s", UserId: 5}})
	activeDelivered.Status = models.PresignDelivered
	Save(activeDelivered)

	RecoverAndClean()

	// Expired credentials are deleted regardless of state.
	_, ok := Get(expiredAvailable.Id)
	test.IsEqualBool(t, ok, false)
	_, ok = database.GetPresign(expiredDelivered.Id)
	test.IsEqualBool(t, ok, false)

	// A stale claim of a crashed instance is released for retry.
	recovered, ok := Get(staleClaimed.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, recovered.Status, models.PresignAvailable)
	retried, ok := TryClaim(staleClaimed.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualBool(t, retried.ClaimToken == "oldtoken", false)

	// A recent claim is left untouched, so live instances keep ownership.
	current, ok := Get(activeClaim.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, current.Status, models.PresignClaimed)
	test.IsEqualString(t, current.ClaimToken, "newtoken")

	// Delivered but unexpired stays terminal.
	current, ok = Get(activeDelivered.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, current.Status, models.PresignDelivered)
}

func TestRestartRecoveryDoesNotStealLiveClaims(t *testing.T) {
	// A very recent claim must survive startup cleanup: during a rolling
	// deploy the old instance may still be draining this delivery.
	p := credential(t, testFiles()[:1])
	claimed, ok := TryClaim(p.Id)
	test.IsEqualBool(t, ok, true)
	RecoverAndClean()
	loaded, ok := Get(p.Id)
	test.IsEqualBool(t, ok, true)
	test.IsEqualInt(t, loaded.Status, models.PresignClaimed)
	test.IsEqualString(t, loaded.ClaimToken, claimed.ClaimToken)
	// A concurrent consumer still cannot enter the delivery.
	_, ok = TryClaim(p.Id)
	test.IsEqualBool(t, ok, false)

	// Once the credential expired (e.g. the holder never recovered), startup
	// cleanup deletes it.
	claimed.Expiry = time.Now().Unix() - 1
	Save(claimed)
	RecoverAndClean()
	_, ok = Get(p.Id)
	test.IsEqualBool(t, ok, false)
}
