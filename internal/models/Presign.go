package models

import "encoding/json"

// Lifecycle states of a presigned download credential.
const (
	// PresignAvailable means the credential has not been claimed yet and can be
	// atomically claimed by exactly one consumer.
	PresignAvailable = iota
	// PresignClaimed means one consumer has checked out the credential, but the
	// response body or redirect has not been committed yet. A failure in this
	// phase releases the credential so the same authorization snapshot can be
	// retried.
	PresignClaimed
	// PresignDelivered is the terminal state: response headers/body or the
	// remote redirect have been committed, so the credential must never deliver
	// content again.
	PresignDelivered
)

// PresignFile is the immutable authorization snapshot of a single file bound
// to a presigned download credential. It freezes the file identity, the
// content version (SHA1) and the owner at issuance time, so that an expired,
// replaced or re-owned file can never be read through an old credential.
type PresignFile struct {
	Id     string `json:"id"`
	SHA1   string `json:"sha1"`
	UserId int    `json:"uid"`
}

// Presign is a one-time, time-limited download credential. It is persisted in
// the database, so that rolling restarts do not invalidate unused credentials
// and all state transitions are atomic across multiple instances.
type Presign struct {
	Id         string        `redis:"Id"`
	UserId     int           `redis:"UserId"`
	Files      []PresignFile `redis:"-"`
	FilesJson  string        `redis:"Files"`
	Filename   string `redis:"Filename"`
	Expiry     int64  `redis:"Expiry"`
	Status     int    `redis:"Status"`
	ClaimedAt  int64  `redis:"ClaimedAt"`
	ClaimToken string `redis:"ClaimToken"`
}

// MarshalFiles serializes the file snapshot into FilesJson. Called before
// persisting a credential.
func (p *Presign) MarshalFiles() error {
	content, err := json.Marshal(p.Files)
	if err != nil {
		return err
	}
	p.FilesJson = string(content)
	return nil
}

// UnmarshalFiles populates Files from the persisted FilesJson snapshot. Called
// after loading a credential from the database. Missing snapshots result in an
// empty file set instead of an error, to stay compatible with empty archives.
func (p *Presign) UnmarshalFiles() error {
	p.Files = nil
	if p.FilesJson == "" {
		return nil
	}
	return json.Unmarshal([]byte(p.FilesJson), &p.Files)
}
