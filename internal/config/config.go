package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// Default values for log level and HTTP port. Centralized so the strings
// don't drift between Load() and tests.
const (
	defaultLogLevel = "info"
	defaultPort     = "8080"
)

type Config struct {
	// Common settings
	BackendType        string
	HTTPTimeout        time.Duration
	HealthCheckTimeout time.Duration
	CacheTTL           time.Duration
	ReWarmInterval     time.Duration // 0 disables the periodic re-warm loop
	Port               string
	LogLevel           string

	// ResticMaxConcurrency caps in-flight `restic` subprocesses. All call
	// sites (CheckBackupExists, ListAllSources, HealthCheck, Connect)
	// acquire from a shared semaphore before exec. 0 means uncapped. The
	// motivation is shared-repo lock contention: many concurrent
	// `restic snapshots --tag` calls queue on repo locks long enough to
	// trip HTTP_TIMEOUT and SIGKILL each other (issue #1 — 503 storm).
	ResticMaxConcurrency int

	// S3 backend settings
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Secure    bool

	// Kopia (S3) backend settings. v3.0.0 removed the filesystem-backed
	// kopia repo (KOPIA_REPOSITORY_PATH); the operator now connects to an
	// S3-compatible backend (e.g. RustFS, MinIO) the same way VolSync mover
	// Jobs do, so there is no shared volume between the operator pod and
	// mover pods. KopiaS3DisableTLS=true is the in-cluster RustFS shape.
	//
	// v3.1.0: KopiaPassword / KopiaS3AccessKey / KopiaS3SecretKey are now
	// OPTIONAL. When KopiaCredentialsPath is set (the operator deployment
	// shape since v3.1.0), credentials are loaded lazily from disk on each
	// kopia subprocess invocation rather than from env vars at pod startup.
	// The legacy cmd/pvc-plumber HTTP-only binary still populates these
	// fields from secretKeyRef env vars and uses the static credentials
	// source — both paths compile and run.
	KopiaPassword     string
	KopiaS3Endpoint   string
	KopiaS3Bucket     string
	KopiaS3AccessKey  string
	KopiaS3SecretKey  string
	KopiaS3DisableTLS bool

	// KopiaCredentialsPath is the directory the operator reads kopia
	// credentials from on each subprocess invocation. Defaults to
	// `/var/secret/pvc-plumber-kopia` (matches the deployment.yaml
	// volumeMount). Each Secret key (KOPIA_PASSWORD, AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY) becomes a separate file under this directory
	// when kubelet renders the Secret as a volume.
	//
	// New in v3.1.0 — fixes the v3.0.0 ES-race pod-startup deadlock where
	// secretKeyRef env-var resolution failed during ArgoCD sync waves
	// because the pvc-plumber-kopia Secret was mid-update. Setting this
	// path makes the operator read creds at call-time instead, so a Secret
	// that hasn't rendered yet doesn't crash the pod.
	KopiaCredentialsPath string

	// KopiaConnectTimeout caps the total time `kopia.Client.Connect()`
	// spends retrying on ErrCredentialsNotReady. Defaults to 60s.
	// Configurable via the KOPIA_CONNECT_TIMEOUT env var. After this
	// elapses, Connect returns an error and controller-runtime re-queues
	// the calling reconcile.
	KopiaConnectTimeout time.Duration

	// Restic (S3) backend settings. The restic backend talks to a single
	// shared S3-backed repository identified by RESTIC_REPOSITORY (e.g.
	// `s3:https://garage.lab.example/volsync-shared/restic`). Snapshots
	// are queried by `<namespace>/<pvc>` tag — see internal/restic and
	// docs/volsync-storage-recovery.md in the consuming GitOps repo.
	//
	// Like the kopia-s3 backend, credentials are loaded lazily from a
	// directory-mounted Secret when ResticCredentialsPath is set; the
	// env-var fallback (StaticCredentialsSource) is retained for legacy
	// HTTP-only deployment shapes.
	ResticRepository      string
	ResticPassword        string
	ResticS3AccessKey     string
	ResticS3SecretKey     string
	ResticCredentialsPath string
	ResticConnectTimeout  time.Duration

	// ResticCacheDir overrides restic's default cache location. Set when
	// the deployment runs with readOnlyRootFilesystem and mounts an
	// emptyDir for cache (Phase 1 finding: under runAsNonRoot=1000 the
	// default `~/.cache` path fails with permission denied).
	ResticCacheDir string

	// ExternalSecret rendering knobs used by the PVC reconciler when it
	// templates the per-PVC `volsync-<pvc>` ExternalSecret. Defaults pin to
	// the reference cluster's 1Password Connect setup (vault item
	// `rustfs`, properties `kopia_password`/`k8s-admin-access-key`/
	// `k8s-admin-secret-key`). Override these for any deployment that uses
	// a different secret store layout.
	ExternalSecretsStoreName             string
	ExternalSecretsVaultKey              string
	ExternalSecretsKopiaPasswordProperty string
	ExternalSecretsS3AccessKeyProperty   string
	ExternalSecretsS3SecretKeyProperty   string
}

func Load() (*Config, error) {
	// Common settings
	backendType := os.Getenv("BACKEND_TYPE")
	if backendType == "" {
		backendType = backend.TypeS3
	}
	if backendType != backend.TypeS3 && backendType != backend.TypeKopiaS3 && backendType != backend.TypeResticS3 {
		return nil, fmt.Errorf("invalid BACKEND_TYPE: %s (must be %q, %q, or %q)",
			backendType, backend.TypeS3, backend.TypeKopiaS3, backend.TypeResticS3)
	}

	// HTTP_TIMEOUT bounds the /exists request context. 3s was the v3.x
	// default; against a multi-source shared restic repo a single
	// `restic snapshots --tag` regularly takes longer than that under any
	// contention. 30s gives plenty of headroom while still bailing on a
	// truly wedged subprocess. See issue #1.
	httpTimeout := 30 * time.Second
	if timeoutStr := os.Getenv("HTTP_TIMEOUT"); timeoutStr != "" {
		duration, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_TIMEOUT: %w", err)
		}
		httpTimeout = duration
	}

	// HEALTH_CHECK_TIMEOUT bounds the readiness probe's inner restic call
	// (was hardcoded 5s in client.go prior to issue #1). 15s default
	// matches the slowest healthy `restic cat config` we've seen under
	// load, with 10s slack.
	healthCheckTimeout := 15 * time.Second
	if timeoutStr := os.Getenv("HEALTH_CHECK_TIMEOUT"); timeoutStr != "" {
		duration, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid HEALTH_CHECK_TIMEOUT: %w", err)
		}
		if duration <= 0 {
			return nil, fmt.Errorf("HEALTH_CHECK_TIMEOUT must be > 0, got %s", timeoutStr)
		}
		healthCheckTimeout = duration
	}

	reWarmInterval := 90 * time.Second
	if intervalStr := os.Getenv("RE_WARM_INTERVAL"); intervalStr != "" {
		duration, err := time.ParseDuration(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid RE_WARM_INTERVAL: %w", err)
		}
		if duration < 0 {
			return nil, fmt.Errorf("RE_WARM_INTERVAL must be >= 0, got %s", intervalStr)
		}
		reWarmInterval = duration
	}

	// CACHE_TTL defaults to RE_WARM_INTERVAL so cache entries stay valid
	// between re-warms. Prior default was a fixed 60s which left a long
	// gap when RE_WARM_INTERVAL was overridden to several minutes — every
	// /exists call in that gap missed cache and stampeded the backend
	// (issue #1). Explicit CACHE_TTL still overrides; setting it shorter
	// than RE_WARM_INTERVAL is supported but discouraged.
	cacheTTL := reWarmInterval
	if cacheTTL == 0 {
		cacheTTL = 60 * time.Second
	}
	if ttlStr, ok := os.LookupEnv("CACHE_TTL"); ok {
		duration, err := time.ParseDuration(ttlStr)
		if err != nil {
			return nil, fmt.Errorf("invalid CACHE_TTL: %w", err)
		}
		cacheTTL = duration
	}

	// RESTIC_MAX_CONCURRENCY caps in-flight restic subprocesses. Default
	// 2 lets the readiness probe and a /exists call coexist without the
	// cache re-warm starving them, while still throttling thundering
	// herds. 0 disables the cap entirely (legacy behavior, not
	// recommended on a shared repo).
	resticMaxConcurrency := 2
	if maxStr := os.Getenv("RESTIC_MAX_CONCURRENCY"); maxStr != "" {
		n, err := strconv.Atoi(maxStr)
		if err != nil {
			return nil, fmt.Errorf("invalid RESTIC_MAX_CONCURRENCY: %w", err)
		}
		if n < 0 {
			return nil, fmt.Errorf("RESTIC_MAX_CONCURRENCY must be >= 0, got %s", maxStr)
		}
		resticMaxConcurrency = n
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = defaultLogLevel
	}

	cfg := &Config{
		BackendType:          backendType,
		HTTPTimeout:          httpTimeout,
		HealthCheckTimeout:   healthCheckTimeout,
		CacheTTL:             cacheTTL,
		ReWarmInterval:       reWarmInterval,
		Port:                 port,
		LogLevel:             logLevel,
		ResticMaxConcurrency: resticMaxConcurrency,
	}

	// Backend-specific validation
	switch backendType {
	case backend.TypeS3:
		if err := loadS3Config(cfg); err != nil {
			return nil, err
		}
	case backend.TypeKopiaS3:
		if err := loadKopiaS3Config(cfg); err != nil {
			return nil, err
		}
	case backend.TypeResticS3:
		if err := loadResticS3Config(cfg); err != nil {
			return nil, err
		}
	}

	// ExternalSecret rendering knobs are independent of the existence
	// backend choice (they configure the per-PVC ES the reconciler renders),
	// so load them unconditionally.
	loadExternalSecretsConfig(cfg)

	return cfg, nil
}

func loadS3Config(cfg *Config) error {
	cfg.S3Endpoint = os.Getenv("S3_ENDPOINT")
	if cfg.S3Endpoint == "" {
		return fmt.Errorf("S3_ENDPOINT is required")
	}

	cfg.S3Bucket = os.Getenv("S3_BUCKET")
	if cfg.S3Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}

	cfg.S3AccessKey = os.Getenv("S3_ACCESS_KEY")
	if cfg.S3AccessKey == "" {
		return fmt.Errorf("S3_ACCESS_KEY is required")
	}

	cfg.S3SecretKey = os.Getenv("S3_SECRET_KEY")
	if cfg.S3SecretKey == "" {
		return fmt.Errorf("S3_SECRET_KEY is required")
	}

	cfg.S3Secure = false
	if secureStr := os.Getenv("S3_SECURE"); secureStr != "" {
		var err error
		cfg.S3Secure, err = strconv.ParseBool(secureStr)
		if err != nil {
			return fmt.Errorf("invalid S3_SECURE: %w", err)
		}
	}

	return nil
}

// loadKopiaS3Config loads the env vars the operator's own kopia client uses
// to connect to the shared Kopia repository over S3. Mirrors the env-var
// surface VolSync mover Jobs see (KOPIA_S3_ENDPOINT, KOPIA_S3_BUCKET,
// KOPIA_S3_DISABLE_TLS, plus optionally KOPIA_PASSWORD / AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY) so secret material can be reused 1:1 between the
// operator pod and the per-PVC kopia-credentials Secrets the reconciler
// creates.
//
// KOPIA_S3_DISABLE_TLS defaults to false; in-cluster RustFS / plaintext-MinIO
// deployments must set it to "true" explicitly.
//
// v3.1.0: the three credential env vars (KOPIA_PASSWORD, AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY) are now OPTIONAL. The operator deployment mounts
// those keys as a Secret directory (KopiaCredentialsPath) and reads them at
// kopia subprocess call time, side-stepping the ArgoCD ES-race that crashed
// pods on v3.0.0 startup. The legacy HTTP-only cmd/pvc-plumber binary keeps
// passing them as env vars and uses StaticCredentialsSource — both paths
// compile and run.
func loadKopiaS3Config(cfg *Config) error {
	cfg.KopiaS3Endpoint = os.Getenv("KOPIA_S3_ENDPOINT")
	if cfg.KopiaS3Endpoint == "" {
		return fmt.Errorf("KOPIA_S3_ENDPOINT is required for kopia-s3 backend")
	}

	cfg.KopiaS3Bucket = os.Getenv("KOPIA_S3_BUCKET")
	if cfg.KopiaS3Bucket == "" {
		return fmt.Errorf("KOPIA_S3_BUCKET is required for kopia-s3 backend")
	}

	// Credentials are optional in v3.1.0+. When KopiaCredentialsPath is set
	// (operator deployment shape) they're loaded from disk; when it isn't
	// (legacy HTTP-only deployment shape) they must come from env vars.
	cfg.KopiaS3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	cfg.KopiaS3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	cfg.KopiaPassword = os.Getenv("KOPIA_PASSWORD")

	cfg.KopiaS3DisableTLS = false
	if disableStr := os.Getenv("KOPIA_S3_DISABLE_TLS"); disableStr != "" {
		v, err := strconv.ParseBool(disableStr)
		if err != nil {
			return fmt.Errorf("invalid KOPIA_S3_DISABLE_TLS: %w", err)
		}
		cfg.KopiaS3DisableTLS = v
	}

	// KOPIA_CREDENTIALS_PATH semantics:
	//   - unset (LookupEnv returns false) → use the operator default
	//     `/var/secret/pvc-plumber-kopia` so a fresh v3.1.0 deployment
	//     gets path-based creds out of the box.
	//   - set to a non-empty value → use it verbatim.
	//   - set to the empty string explicitly → disable the path-based
	//     loader and require env-var creds (legacy HTTP-only deployment
	//     shape). This is the escape hatch for v1.x callers that haven't
	//     adopted the Secret-mount pattern.
	if v, ok := os.LookupEnv("KOPIA_CREDENTIALS_PATH"); ok {
		cfg.KopiaCredentialsPath = v
	} else {
		cfg.KopiaCredentialsPath = "/var/secret/pvc-plumber-kopia"
	}

	cfg.KopiaConnectTimeout = 60 * time.Second
	if v := os.Getenv("KOPIA_CONNECT_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid KOPIA_CONNECT_TIMEOUT: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("KOPIA_CONNECT_TIMEOUT must be > 0, got %s", v)
		}
		cfg.KopiaConnectTimeout = d
	}

	// Either path-based creds OR env-var creds must be available. The
	// path-based default above is set unconditionally; we only trip an
	// error if KOPIA_CREDENTIALS_PATH was explicitly set to empty AND the
	// env-var trio is also incomplete. This keeps the v3.0.0 → v3.1.0
	// upgrade smooth (legacy env vars still loaded if present) while
	// allowing pure-Secret-mount deployments.
	if cfg.KopiaCredentialsPath == "" {
		if cfg.KopiaPassword == "" {
			return fmt.Errorf("KOPIA_PASSWORD is required for kopia-s3 backend when KOPIA_CREDENTIALS_PATH is empty")
		}
		if cfg.KopiaS3AccessKey == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID is required for kopia-s3 backend when KOPIA_CREDENTIALS_PATH is empty")
		}
		if cfg.KopiaS3SecretKey == "" {
			return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required for kopia-s3 backend when KOPIA_CREDENTIALS_PATH is empty")
		}
	}

	return nil
}

// loadResticS3Config loads the env vars the operator's restic backend uses
// to talk to the shared restic repository. RESTIC_REPOSITORY is the full
// restic repo URL (e.g. `s3:https://garage.lab.example/volsync-shared/
// restic`) — restic encodes endpoint, bucket, and tls choice into this
// URL so there's no separate ENDPOINT / BUCKET / DISABLE_TLS triple.
//
// Credentials follow the same v3.1.0 pattern as the kopia backend:
// RESTIC_CREDENTIALS_PATH (default `/var/secret/pvc-plumber-restic`)
// points at a directory-mounted Secret with one file per key
// (RESTIC_PASSWORD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY). Setting
// RESTIC_CREDENTIALS_PATH to the empty string disables the path loader
// and requires the env-var trio.
func loadResticS3Config(cfg *Config) error {
	cfg.ResticRepository = os.Getenv("RESTIC_REPOSITORY")
	if cfg.ResticRepository == "" {
		return fmt.Errorf("RESTIC_REPOSITORY is required for restic-s3 backend")
	}

	cfg.ResticS3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	cfg.ResticS3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	cfg.ResticPassword = os.Getenv("RESTIC_PASSWORD")

	if v, ok := os.LookupEnv("RESTIC_CREDENTIALS_PATH"); ok {
		cfg.ResticCredentialsPath = v
	} else {
		cfg.ResticCredentialsPath = "/var/secret/pvc-plumber-restic"
	}

	cfg.ResticConnectTimeout = 60 * time.Second
	if v := os.Getenv("RESTIC_CONNECT_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid RESTIC_CONNECT_TIMEOUT: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("RESTIC_CONNECT_TIMEOUT must be > 0, got %s", v)
		}
		cfg.ResticConnectTimeout = d
	}

	// RESTIC_CACHE_DIR is optional; empty value lets restic pick its
	// default ($XDG_CACHE_HOME/restic, typically $HOME/.cache/restic).
	// The deployment shape under runAsNonRoot=1000 with a read-only root
	// FS must set this to a writable emptyDir mount or restic will fail.
	cfg.ResticCacheDir = os.Getenv("RESTIC_CACHE_DIR")

	// Either path-based creds OR env-var creds must be available — same
	// rule as loadKopiaS3Config. The path default above is set
	// unconditionally; we only trip an error if RESTIC_CREDENTIALS_PATH
	// was explicitly set to empty AND the env-var trio is also incomplete.
	if cfg.ResticCredentialsPath == "" {
		if cfg.ResticPassword == "" {
			return fmt.Errorf("RESTIC_PASSWORD is required for restic-s3 backend when RESTIC_CREDENTIALS_PATH is empty")
		}
		if cfg.ResticS3AccessKey == "" {
			return fmt.Errorf("AWS_ACCESS_KEY_ID is required for restic-s3 backend when RESTIC_CREDENTIALS_PATH is empty")
		}
		if cfg.ResticS3SecretKey == "" {
			return fmt.Errorf("AWS_SECRET_ACCESS_KEY is required for restic-s3 backend when RESTIC_CREDENTIALS_PATH is empty")
		}
	}

	return nil
}

// loadExternalSecretsConfig reads the (optional, defaulted) env vars that
// parameterize how the PVC reconciler renders each `volsync-<pvc>`
// ExternalSecret. Defaults match the reference cluster's 1Password Connect
// layout (vault item `rustfs`, properties for the kopia password and the
// S3 admin keys). Anyone running on a different secret store overrides
// these per-deployment; the env-var surface is the only place that name
// "1password" / "rustfs" / "kopia_password" appears.
func loadExternalSecretsConfig(cfg *Config) {
	cfg.ExternalSecretsStoreName = os.Getenv("EXTERNAL_SECRETS_STORE_NAME")
	if cfg.ExternalSecretsStoreName == "" {
		cfg.ExternalSecretsStoreName = "1password"
	}
	cfg.ExternalSecretsVaultKey = os.Getenv("EXTERNAL_SECRETS_VAULT_KEY")
	if cfg.ExternalSecretsVaultKey == "" {
		cfg.ExternalSecretsVaultKey = "rustfs"
	}
	cfg.ExternalSecretsKopiaPasswordProperty = os.Getenv("EXTERNAL_SECRETS_KOPIA_PASSWORD_PROPERTY")
	if cfg.ExternalSecretsKopiaPasswordProperty == "" {
		cfg.ExternalSecretsKopiaPasswordProperty = "kopia_password"
	}
	cfg.ExternalSecretsS3AccessKeyProperty = os.Getenv("EXTERNAL_SECRETS_S3_ACCESS_KEY_PROPERTY")
	if cfg.ExternalSecretsS3AccessKeyProperty == "" {
		cfg.ExternalSecretsS3AccessKeyProperty = "k8s-admin-access-key"
	}
	cfg.ExternalSecretsS3SecretKeyProperty = os.Getenv("EXTERNAL_SECRETS_S3_SECRET_KEY_PROPERTY")
	if cfg.ExternalSecretsS3SecretKeyProperty == "" {
		cfg.ExternalSecretsS3SecretKeyProperty = "k8s-admin-secret-key"
	}
}
