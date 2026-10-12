package filerequest

import (
	"time"

	"github.com/forceu/gokapi/internal/configuration"
	"github.com/forceu/gokapi/internal/configuration/database"
	"github.com/forceu/gokapi/internal/helper"
	"github.com/forceu/gokapi/internal/models"
	"github.com/forceu/gokapi/internal/storage"
	"github.com/forceu/gokapi/internal/storage/chunking/chunkreservation"
)

// New creates a new file request object. It is not stored yet,
// and an API key has to be generated manually
func New(user models.User) models.FileRequest {
	return models.FileRequest{
		Id:           helper.GenerateRandomString(configuration.GetEnvironment().LengthId),
		UserId:       user.Id,
		CreationDate: time.Now().Unix(),
		Version:      1,
		Name:         "Unnamed file request",
	}
}

// Get returns a file request object by its ID and populates it
func Get(id string) (models.FileRequest, bool) {
	result, ok := database.GetFileRequest(id)
	if !ok {
		return models.FileRequest{}, false
	}
	result.Populate(database.GetAllMetadata(), configuration.Get().MaxFileSizeMB,
		database.CountActiveUploadReservations(id))
	return result, true
}

// GetAll returns a list of all file requests and populates them
func GetAll() []models.FileRequest {
	result := database.GetAllFileRequests()
	if len(result) == 0 {
		return result
	}
	allFiles := database.GetAllMetadata()
	maxServerSize := configuration.Get().MaxFileSizeMB
	for i, request := range result {
		request.Populate(allFiles, maxServerSize,
			database.CountActiveUploadReservations(request.Id))
		result[i] = request
	}
	return result
}

// Delete all files associated with a file request, its open reservations and the request itself
func Delete(request models.FileRequest) {
	files := GetAllFiles(request)
	storage.DeleteFiles(files, true)
	chunkreservation.CancelAllForRequest(request.Id)
	database.DeleteFileRequest(request)
	database.DeleteApiKey(request.ApiKey)
}

// GetAllFiles returns a list of all files associated with a file request
func GetAllFiles(request models.FileRequest) []models.File {
	var result []models.File
	files := database.GetAllMetadata()
	for _, file := range files {
		if file.UploadRequestId == request.Id {
			result = append(result, file)
		}
	}
	return result
}
