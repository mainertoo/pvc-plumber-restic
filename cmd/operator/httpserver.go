// HTTP server construction for the operator binary.
//
// This mirrors cmd/pvc-plumber/main.go's setup so the operator can host the
// existing read-only `/exists/`, `/healthz`, `/readyz`, `/metrics` surface
// alongside the new controller-runtime manager. The two share a single
// backend + cache instance, so webhook handlers and HTTP callers benefit
// from one connection to Kopia and one cached decision per (ns, pvc).
//
// The function below is intentionally a near-copy of the equivalent block
// in cmd/pvc-plumber/main.go — keeping them duplicate for now (rather than
// extracting to a shared internal package) preserves the legacy binary
// untouched until the operator has soaked through Phase 4 cutover.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
	"github.com/mitchross/pvc-plumber/internal/cache"
	"github.com/mitchross/pvc-plumber/internal/config"
	"github.com/mitchross/pvc-plumber/internal/handler"
	"github.com/mitchross/pvc-plumber/internal/kopia"
	"github.com/mitchross/pvc-plumber/internal/restic"
	"github.com/mitchross/pvc-plumber/internal/s3"
)

// backendBundle groups the constructed backend, the cache wrapper that
// fronts it, and (when applicable) the source lister that drives the
// periodic cache re-warm. The lister is nil for BACKEND_TYPE=s3 — only
// kopia-s3 and restic-s3 expose ListAllSources.
type backendBundle struct {
	backend handler.BackendClient
	cached  *cache.CachedClient
	lister  backend.SourceLister
}

// buildBackend constructs the backend client + cache layer. Returns the
// cached client (which the operator passes to webhook handlers as their
// `kopiaClient`) plus a sourceLister when the chosen backend supports
// pre-warm/re-warm (kopia-s3, restic-s3).
//
// On BACKEND_TYPE=s3 the lister is nil; the cache layer wraps the S3
// client directly and populates on demand. The webhook layer doesn't
// care which backend it's talking to as long as the BackendClient.Check-
// BackupExists contract holds.
func buildBackend(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*backendBundle, error) {
	var backendClient handler.BackendClient
	var lister backend.SourceLister

	switch cfg.BackendType {
	case backend.TypeS3:
		logger.Info("initializing s3 backend",
			"endpoint", cfg.S3Endpoint,
			"bucket", cfg.S3Bucket,
			"secure", cfg.S3Secure)
		s3Client, err := s3.NewClient(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Secure)
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		backendClient = s3Client

	case backend.TypeKopiaS3:
		logger.Info("initializing kopia-s3 backend",
			"endpoint", cfg.KopiaS3Endpoint,
			"bucket", cfg.KopiaS3Bucket,
			"disable_tls", cfg.KopiaS3DisableTLS,
			"credentials_path", cfg.KopiaCredentialsPath,
			"connect_timeout", cfg.KopiaConnectTimeout,
		)
		var creds kopia.CredentialsSource
		if cfg.KopiaCredentialsPath != "" {
			creds = kopia.NewDirCredentialsSource(cfg.KopiaCredentialsPath)
		} else {
			creds = kopia.NewStaticCredentialsSource(cfg.KopiaPassword, cfg.KopiaS3AccessKey, cfg.KopiaS3SecretKey)
		}
		kc := kopia.NewClient(kopia.S3Config{
			Endpoint:   cfg.KopiaS3Endpoint,
			Bucket:     cfg.KopiaS3Bucket,
			DisableTLS: cfg.KopiaS3DisableTLS,
		}, creds, logger, kopia.Options{ConnectTimeout: cfg.KopiaConnectTimeout})
		if err := kc.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect to kopia repository: %w", err)
		}
		lister = kc
		backendClient = kc

	case backend.TypeResticS3:
		logger.Info("initializing restic-s3 backend",
			"repository", cfg.ResticRepository,
			"credentials_path", cfg.ResticCredentialsPath,
			"connect_timeout", cfg.ResticConnectTimeout,
			"cache_dir", cfg.ResticCacheDir,
		)
		// Same dir-Secret-vs-env-var credential-source selection as the
		// kopia path. The deployment shape sets ResticCredentialsPath
		// to the mounted volsync-shared Secret directory; the legacy
		// HTTP-only deployment shape may pass creds via env vars
		// instead.
		var creds restic.CredentialsSource
		if cfg.ResticCredentialsPath != "" {
			creds = restic.NewDirCredentialsSource(cfg.ResticCredentialsPath)
		} else {
			creds = restic.NewStaticCredentialsSource(cfg.ResticPassword, cfg.ResticS3AccessKey, cfg.ResticS3SecretKey)
		}
		rc := restic.NewClient(restic.RepoConfig{
			Repository: cfg.ResticRepository,
			CacheDir:   cfg.ResticCacheDir,
		}, creds, logger, restic.Options{
			ConnectTimeout:     cfg.ResticConnectTimeout,
			HealthCheckTimeout: cfg.HealthCheckTimeout,
			MaxConcurrency:     cfg.ResticMaxConcurrency,
		})
		if err := rc.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect to restic repository: %w", err)
		}
		lister = rc
		backendClient = rc

	default:
		return nil, fmt.Errorf("invalid BACKEND_TYPE: %s", cfg.BackendType)
	}

	cachedBackend := cache.New(backendClient, cfg.CacheTTL, logger, cfg.BackendType)

	// Pre-warm only on backends that can enumerate sources (kopia-s3,
	// restic-s3). Failure is non-fatal — the cache populates on demand.
	if lister != nil {
		sources, err := lister.ListAllSources(ctx)
		if err != nil {
			logger.Warn("cache pre-warm failed, will populate on demand", "error", err)
		} else {
			cachedBackend.PreWarm(sources)
		}
	}

	return &backendBundle{
		backend: backendClient,
		cached:  cachedBackend,
		lister:  lister,
	}, nil
}

// newHTTPServer wires the existing /exists, /healthz, /readyz, /metrics
// routes onto a *http.Server bound to cfg.Port. The caller is responsible
// for ListenAndServe + Shutdown.
func newHTTPServer(cfg *config.Config, b *backendBundle, logger *slog.Logger) *http.Server {
	var healthChecker handler.HealthChecker
	if hc, ok := b.backend.(handler.HealthChecker); ok {
		healthChecker = hc
	}
	h := handler.NewWithHealthChecker(b.cached, healthChecker, logger)
	h.SetRequestTimeout(cfg.HTTPTimeout)

	mux := http.NewServeMux()
	mux.HandleFunc("/exists/", h.HandleExists)
	mux.HandleFunc("/healthz", h.HandleHealthz)
	mux.HandleFunc("/readyz", h.HandleReadyz)
	mux.HandleFunc("/metrics", h.HandleMetrics)

	return &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// runCacheReWarmLoop periodically re-runs the backend's source listing
// and refreshes the cache so deleted backups stop returning stale
// exists=true within one re-warm cycle. Takes a sourceLister rather
// than a typed *kopia.Client so the same loop drives kopia-s3 and
// restic-s3 backends. Returns when ctx is canceled.
func runCacheReWarmLoop(
	ctx context.Context,
	lister backend.SourceLister,
	cachedBackend *cache.CachedClient,
	interval time.Duration,
	logger *slog.Logger,
) {
	logger.Info("cache re-warm loop starting", "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	callTimeout := interval
	if callTimeout > 60*time.Second {
		callTimeout = 60 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("cache re-warm loop stopping")
			return
		case <-ticker.C:
			callCtx, cancel := context.WithTimeout(ctx, callTimeout)
			sources, err := lister.ListAllSources(callCtx)
			cancel()
			if err != nil {
				logger.Warn("cache re-warm failed; keeping previous entries", "error", err)
				continue
			}
			cachedBackend.Refresh(sources)
		}
	}
}
