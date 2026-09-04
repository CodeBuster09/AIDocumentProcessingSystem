package api

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"docpipe/internal/domain"
	"docpipe/internal/storage"
)

const pdfMagic = "%PDF-"

type uploadResponse struct {
	DocumentID string `json:"documentId"`
	Status     string `json:"status"`
	StatusURL  string `json:"statusUrl"`
}

func (s *server) uploadDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uid := userID(ctx)

	// Cap the body BEFORE anything reads it. Without this the multipart
	// parser will happily spool a 10 GB upload to disk first and reject it
	// after. Order matters more than the limit itself.
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxUploadBytes)

	file, header, err := r.FormFile("file")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeProblem(w, http.StatusRequestEntityTooLarge, "FILE_TOO_LARGE",
				"File too large",
				"Maximum upload size is "+
					strconv.FormatInt(s.cfg.MaxUploadBytes/(1<<20), 10)+" MB.")
			return
		}
		badRequest(w, `Expected a multipart form with a "file" field.`)
		return
	}
	defer file.Close()

	// Trust the bytes, not the extension or the client's Content-Type.
	magic := make([]byte, len(pdfMagic))
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != pdfMagic {
		badRequest(w, "File is not a PDF.")
		return
	}

	// multipart.File is an io.ReadSeeker — Go already spilled anything large
	// to a temp file for you. So hash it, rewind, and upload the same handle.
	// No second temp file of your own.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		internalError(w, err)
		return
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		internalError(w, err)
		return
	}
	checksum := hex.EncodeToString(hasher.Sum(nil))

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		internalError(w, err)
		return
	}

	docID := uuid.New()
	key := storage.Key(uid, docID)

	if err := s.blobs.Put(ctx, key, file, header.Size, "application/pdf"); err != nil {
		internalError(w, err)
		return
	}

	doc := &domain.Document{
		ID:             docID,
		UserID:         uid,
		Filename:       header.Filename,
		StorageBucket:  s.cfg.MinioBucket,
		StorageKey:     key,
		ContentType:    "application/pdf",
		FileSize:       header.Size,
		ChecksumSHA256: checksum,
		Status:         domain.StatusUploaded,
	}

	if err := s.docs.Insert(ctx, doc); err != nil {
		// Compensating action: the object landed but the row didn't, so the
		// object is now unreferenced. Best-effort cleanup — if this delete
		// also fails, phase 4's sweeper is what actually guarantees it.
		if delErr := s.blobs.Delete(ctx, key); delErr != nil {
			slog.Error("orphaned object", "key", key, "err", delErr)
		}
		if errors.Is(err, domain.ErrDuplicate) {
			writeProblem(w, http.StatusConflict, "DUPLICATE_DOCUMENT",
				"Already uploaded",
				"You have already uploaded this file.")
			return
		}
		internalError(w, err)
		return
	}

	// 202, not 201: the document exists, but the work it implies has not
	// happened. In phase 3 the job publish goes here.
	w.Header().Set("Location", "/api/v1/documents/"+docID.String()+"/status")
	writeJSON(w, http.StatusAccepted, uploadResponse{
		DocumentID: docID.String(),
		Status:     string(domain.StatusUploaded),
		StatusURL:  "/api/v1/documents/" + docID.String() + "/status",
	})
}

func (s *server) getDocument(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		notFound(w) // a malformed id is indistinguishable from a missing one
		return
	}

	doc, err := s.docs.GetByID(r.Context(), id, userID(r.Context()))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			notFound(w)
			return
		}
		internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(doc))
}

func (s *server) listDocuments(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}

	docs, next, err := s.docs.ListByUser(
		r.Context(), userID(r.Context()), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		internalError(w, err)
		return
	}

	items := make([]documentDTO, 0, len(docs))
	for i := range docs {
		items = append(items, toDTO(&docs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"nextCursor": next,
	})
}

func (s *server) downloadDocument(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		notFound(w)
		return
	}

	doc, err := s.docs.GetByID(r.Context(), id, userID(r.Context()))
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			notFound(w)
			return
		}
		internalError(w, err)
		return
	}

	url, err := s.blobs.PresignGet(r.Context(), doc.StorageKey, doc.Filename, 60*time.Second)
	if err != nil {
		internalError(w, err)
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}

// documentDTO is separate from domain.Document on purpose: storage_key,
// lease_owner and checksum are internal and must never reach the client.
type documentDTO struct {
	ID        string    `json:"id"`
	Filename  string    `json:"filename"`
	Status    string    `json:"status"`
	FileSize  int64     `json:"fileSize"`
	PageCount *int      `json:"pageCount,omitempty"`
	Summary   *string   `json:"summary,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// toDTO converts a domain.Document to a documentDTO for API responses.
// The conversion is explicit to avoid accidentally exposing internal fields.
func toDTO(d *domain.Document) documentDTO {
	return documentDTO{
		ID:        d.ID.String(),
		Filename:  d.Filename,
		Status:    string(d.Status),
		FileSize:  d.FileSize,
		PageCount: d.PageCount,
		Summary:   d.Summary,
		CreatedAt: d.CreatedAt,
	}
}