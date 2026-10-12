package models

// UploadParameters is used to set parameters for a new upload
type UploadParameters struct {
	UserId              int
	AllowedDownloads    int
	Expiry              int
	MaxMemory           int
	ExpiryTimestamp     int64
	RealSize            int64
	UnlimitedDownload   bool
	UnlimitedTime       bool
	IsEndToEndEncrypted bool
	Password            string
	ExternalUrl         string
	FileRequestId       string
	// PreassignedFileId is used for file request uploads to reuse the file id that was
	// stored on the reservation when the completion was claimed. This keeps completion
	// idempotent if the storage or database response is uncertain.
	PreassignedFileId string
}
