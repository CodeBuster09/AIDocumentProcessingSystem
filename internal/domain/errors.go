package domain

import "errors"

// Sentinel errors live here because both store and api must agree on them,
// and api must not import store. This package is the shared vocabulary.
var (
	ErrNotFound      = errors.New("not found")
	ErrDuplicate     = errors.New("duplicate document")
	ErrInvalidCursor = errors.New("invalid cursor")
)
