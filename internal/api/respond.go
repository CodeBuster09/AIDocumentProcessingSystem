package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Status is already sent, so this can only be logged.
		slog.Error("encode response", "err", err)
	}
}

// problem is RFC 7807. `code` is the stable machine-readable part — the
// frontend branches on it, so it must not change when you reword `title`.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

func writeProblem(w http.ResponseWriter, status int, code, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:   "https://docpipe.local/errors/" + code,
		Title:  title,
		Status: status,
		Code:   code,
		Detail: detail,
	})
}

func badRequest(w http.ResponseWriter, detail string) {
	writeProblem(w, http.StatusBadRequest, "VALIDATION_FAILED", "Invalid request", detail)
}

func notFound(w http.ResponseWriter) {
	writeProblem(w, http.StatusNotFound, "NOT_FOUND", "Document not found", "")
}

func internalError(w http.ResponseWriter, err error) {
	slog.Error("internal", "err", err)
	writeProblem(w, http.StatusInternalServerError, "INTERNAL",
		"Something went wrong", "") // never leak err to the client
}
