package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotFound lets callers tell "this object is gone" apart from "the
// object store is unreachable". The phase 3 worker needs that distinction:
// a missing object is permanent and must not be retried five times.
var ErrNotFound = errors.New("object not found")

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

type Store struct {
	client *minio.Client
	bucket string
}

func NewMinioClient(ctx context.Context, cfg Config) (*Store, error) {

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	// Ensure the bucket exists or create it if it doesn't
	s := &Store{client: client, bucket: cfg.Bucket}
	if err := s.ensureBucket(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure bucket: %w", err)
	}

	return s, nil
}

func (s *Store) ensureBucket(ctx context.Context) error {
	// First call that touches the network, so a wrong endpoint or a stopped
	// container surfaces here at startup rather than on the first upload.
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", s.bucket, err)
	}
	if exists {
		return nil
	}

	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
		// In phase 3 the api and worker start together and can both see
		// "not exists" and both try to create it. Losing that race is fine.
		if minio.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", s.bucket, err)
	}
	return nil
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, fmt.Errorf("%q: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("stat %q: %w", key, err)
	}
	return obj, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key,
		minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

func Key(userID, documentID uuid.UUID) string {
	return fmt.Sprintf("%s/%s.pdf", userID, documentID)
}

// PresignGet returns a short-lived URL the browser can fetch directly, so
// the PDF bytes never pass through your API on download.
func (s *Store) PresignGet(
	ctx context.Context,
	key, downloadName string,
	ttl time.Duration,
) (string, error) {
	// Without this the browser saves the file as the UUID key. This makes it
	// arrive as the name the user originally uploaded.
	params := url.Values{}
	params.Set("response-content-disposition",
		fmt.Sprintf("attachment; filename=%q", downloadName))

	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, params)
	if err != nil {
		return "", fmt.Errorf("presign %q: %w", key, err)
	}
	return u.String(), nil
}
