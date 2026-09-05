package domain

import (
	"time"

	"github.com/google/uuid"
)

// Status mirrors the doc_status enum in Postgres. Typed rather than a bare
// string so a typo is a compile error, not a row that never matches.
type Status string

const (
	StatusUploaded   Status = "UPLOADED"   // stored, not yet claimed
	StatusProcessing Status = "PROCESSING" // a worker holds the lease
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

// IsTerminal reports whether the pipeline is done with this document.
// Phase 3 uses it to decide whether a redelivered job is worth running.
func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed
}

type Document struct {
	ID             uuid.UUID
	UserID         uuid.UUID
	Filename       string
	StorageBucket  string
	StorageKey     string
	ContentType    string
	FileSize       int64
	ChecksumSHA256 string
	Status         Status

	// Pointers because these are NULL until the worker fills them in.
	// With plain values, "summary is empty string" and "summary hasn't been
	// generated yet" become the same thing.
	PageCount     *int
	Summary       *string
	FailureCode   *string
	FailureDetail *string

	CreatedAt time.Time
	UpdatedAt time.Time
}
