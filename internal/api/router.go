package api

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"docpipe/internal/config"
	"docpipe/internal/domain"
)

type documentStore interface {
	Insert(ctx context.Context, d *domain.Document) error
	GetByID(ctx context.Context, id, userID uuid.UUID) (*domain.Document, error)
	ListByUser(ctx context.Context, userID uuid.UUID, limit int, cursor string) (
		[]domain.Document, string, error)
}

type blobStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	PresignGet(ctx context.Context, key, downloadName string, ttl time.Duration) (string, error)
	Delete(ctx context.Context, key string) error
}

type server struct {
	cfg   config.Config
	docs  documentStore
	blobs blobStore
}

func NewRouter(cfg config.Config, docs documentStore, blobs blobStore) http.Handler {
	s := &server{
		cfg:   cfg,
		docs:  docs,
		blobs: blobs,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Use(injectDevUser(cfg.DevUserID))

		r.Route("/documents", func(r chi.Router) {
			r.Post("/", s.handleUploadDocument)
			r.Get("/", s.handleListDocuments)
			r.Get("/{id}", s.handleGetDocument)
			r.Get("/{id}/download", s.handleDownloadDocument)
		})
	})

	return r
}
