package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"docpipe/internal/api"
	"docpipe/internal/config"
	"docpipe/internal/storage"
	"docpipe/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}
}

// run exists so deferred cleanup actually runs. os.Exit skips defers, so main
// stays a three-line wrapper and everything else returns errors.
func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// Cancelled on Ctrl-C / SIGTERM. Everything that should stop when the
	// process is asked to stop hangs off this context.
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Postgres ────────────────────────────────────────────────────────

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	poolCfg.MaxConns = 10
	poolCfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	// NewWithConfig is lazy — it doesn't dial. Ping so a bad DATABASE_URL
	// fails at startup instead of on the first user request.
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	slog.Info("postgres connected")

	// ── Migrations ──────────────────────────────────────────────────────

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// ── Object storage ──────────────────────────────────────────────────

	blobs, err := storage.NewMinioClient(ctx, storage.Config{
		Endpoint:  cfg.MinioEndpoint,
		AccessKey: cfg.MinioAccessKey,
		SecretKey: cfg.MinioSecretKey,
		Bucket:    cfg.MinioBucket,
		UseSSL:    cfg.MinioUseSSL,
	})
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	slog.Info("minio ready", "bucket", cfg.MinioBucket)

	// ── Wiring ──────────────────────────────────────────────────────────

	documents := store.NewDocuments(pool)
	handler := api.NewRouter(cfg, documents, blobs)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// Deliberately no WriteTimeout: it's an absolute deadline on the whole
		// exchange, so a 25 MB upload over a slow connection would be cut off
		// mid-transfer. Bound individual handlers instead.
	}

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	// Background(), NOT ctx — ctx is already cancelled here, and passing it
	// makes Shutdown return instantly and drop in-flight uploads. This is the
	// single easiest graceful-shutdown bug to write.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	slog.Info("stopped cleanly")
	return nil
}
