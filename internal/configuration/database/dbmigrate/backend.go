package dbmigrate

import "github.com/forceu/gokapi/internal/models"

// Object kinds that are copied by a migration. Every record of a migration
// generation is tagged with one of these kinds.
const (
	kindUser        = "user"
	kindE2E         = "e2e"
	kindApiKey      = "apikey"
	kindFile        = "file"
	kindHotlink     = "hotlink"
	kindFileRequest = "filerequest"
)

// allKinds lists every migrated kind in copy order.
var allKinds = []string{kindUser, kindE2E, kindApiKey, kindFileRequest, kindFile, kindHotlink}

// StagingBackend is implemented by database providers that support
// generation-isolated, resumable migration. All staging data is written under
// the passed migration generation id and is never visible to regular database
// reads until MigActivate publishes the generation atomically.
type StagingBackend interface {
	// MigPrepare ensures that migration state and staging storage exist.
	MigPrepare() error

	// MigLoadState returns the persisted migration state JSON. The boolean
	// is false if no migration has ever been started against the destination.
	MigLoadState() ([]byte, bool, error)
	// MigCreateState persists the state of a new migration generation only if
	// no state exists yet. It returns true if the state was created.
	MigCreateState(migrationId string, stateBlob []byte) (bool, error)
	// MigSaveStateCAS overwrites the state only if the stored migration id
	// equals expectedId, preventing concurrent or newer runs from overwriting
	// each other.
	MigSaveStateCAS(expectedId string, stateBlob []byte) error

	// Staging writers. A writer never touches live data.
	MigStageUser(genId string, user models.User) error
	MigStageApiKey(genId string, apiKey models.ApiKey) error
	MigStageEnd2End(genId string, userId int, info models.E2EInfoEncrypted) error
	MigStageFile(genId string, file models.File) error
	MigStageHotlink(genId string, hotlinkId string, fileId string) error
	MigStageFileRequest(genId string, request models.FileRequest) error
	MigStageUserIdMax(genId string, maxUserId int) error

	// Staging readers, used for reconciliation and integrity verification.
	MigGetStagedUser(genId string, userId int) (models.User, bool, error)
	MigGetStagedApiKey(genId string, id string) (models.ApiKey, bool, error)
	MigGetStagedEnd2End(genId string, userId int) (models.E2EInfoEncrypted, bool, error)
	MigGetStagedFile(genId string, id string) (models.File, bool, error)
	MigGetStagedHotlink(genId string, id string) (string, bool, error)
	MigGetStagedFileRequest(genId string, id string) (models.FileRequest, bool, error)
	MigGetStagedUserIdMax(genId string) (int, bool, error)
	// MigStagedCounts returns the number of staged records per kind.
	MigStagedCounts(genId string) (map[string]int, error)

	// MigActivate publishes the staged generation atomically: live data of
	// migrated kinds becomes exactly the staged content, while sessions and
	// other non-migrated data remain untouched. On failure the previously
	// usable live data must remain unchanged.
	MigActivate(genId string, schemaVersion int, apiKeyExpiries map[string]int64) error
	// MigDropStaging removes the staging data of a generation after it has
	// been activated. The migration state is kept for idempotent reruns.
	MigDropStaging(genId string) error
}
