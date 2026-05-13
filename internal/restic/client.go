// Package restic implements a backend that answers /exists queries by
// shelling out to the restic CLI against a single shared S3-backed
// repository. Snapshots are identified by tag — the tag schema is
// `<namespace>/<pvc>`, written by every VolSync mover Job in the
// label-driven design (see docs/volsync-storage-recovery.md in the
// consuming GitOps repo).
//
// Compared to the sibling kopia backend, restic has no persistent on-disk
// session — each subprocess gets repo URL + password via env vars and runs
// hermetically. That eliminates kopia's connect-then-reuse pattern (and the
// $KOPIA_CONFIG_PATH state file) but means Connect() is purely a one-shot
// validation; the `connected` flag exists only so HealthCheck has a
// startup-completed gate matching the kopia client's contract.
package restic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

// CommandExecutor interface for running commands (enables testing).
// Diverges from kopia.CommandExecutor by carrying an explicit env slice —
// restic accepts RESTIC_REPOSITORY / RESTIC_PASSWORD / AWS_* via env vars
// rather than argv flags, and creds may rotate between calls in v3.1.0+
// lazy-load mode.
type CommandExecutor interface {
	Run(ctx context.Context, env []string, name string, args ...string) ([]byte, error)
}

// RealExecutor executes commands using os/exec.
type RealExecutor struct{}

func (e *RealExecutor) Run(ctx context.Context, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	return cmd.Output()
}

// Creds bundles the three credential strings every restic subprocess needs.
type Creds struct {
	Password  string
	AccessKey string
	SecretKey string
}

// restic CLI subcommand literals — extracted as constants because they
// appear in both the connect probe path and the HealthCheck status probe,
// matching the kopia client's convention.
const (
	resticCmdSnapshots = "snapshots"
	resticCmdCat       = "cat"
)

// CredentialsSource hides where restic credentials come from. Mirrors
// kopia.CredentialsSource — see that package's doc-comment for the
// ESO-race motivation. Load is called on every subprocess invocation that
// needs creds so a Secret update from the External Secrets Operator is
// picked up on the next call without restarting the operator pod.
type CredentialsSource interface {
	Load() (Creds, error)
}

// ErrCredentialsNotReady is returned by a CredentialsSource when the
// backing material isn't available yet (file missing, file empty, Secret
// not rendered, …). Callers distinguish this from "restic repo error" via
// errors.Is — on this error class we retry with backoff rather than
// failing the operation.
var ErrCredentialsNotReady = errors.New("restic credentials not ready")

// DirCredentialsSource reads each credential from a separate file under a
// directory, the shape kubelet writes when a Secret is mounted as a
// volume. Each key in the Secret becomes a file with the same name (e.g.
// RESTIC_PASSWORD → <dir>/RESTIC_PASSWORD).
type DirCredentialsSource struct {
	Dir string
}

// NewDirCredentialsSource constructs a CredentialsSource that reads files
// from the given mount directory. The default mount path
// (`/var/secret/pvc-plumber-restic`) lines up with the deployment.yaml
// volumeMount in the consuming GitOps repo.
func NewDirCredentialsSource(dir string) *DirCredentialsSource {
	return &DirCredentialsSource{Dir: dir}
}

// Load reads the three credential files. On any read failure or empty
// value, returns ErrCredentialsNotReady wrapping the underlying error so
// the caller can backoff-and-retry rather than treating it as a hard
// failure. Trailing whitespace (newline left by `kubectl create secret`
// etc.) is trimmed.
func (d *DirCredentialsSource) Load() (Creds, error) {
	if d.Dir == "" {
		return Creds{}, fmt.Errorf("%w: credentials path is empty", ErrCredentialsNotReady)
	}
	pw, err := readSecretFile(filepath.Join(d.Dir, "RESTIC_PASSWORD"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	ak, err := readSecretFile(filepath.Join(d.Dir, "AWS_ACCESS_KEY_ID"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	sk, err := readSecretFile(filepath.Join(d.Dir, "AWS_SECRET_ACCESS_KEY"))
	if err != nil {
		return Creds{}, fmt.Errorf("%w: %w", ErrCredentialsNotReady, err)
	}
	return Creds{Password: pw, AccessKey: ak, SecretKey: sk}, nil
}

// readSecretFile reads a single Secret file and trims trailing whitespace.
// An empty file is treated the same as a missing file — both surface as
// "not ready".
func readSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	v := strings.TrimRight(string(b), "\r\n\t ")
	if v == "" {
		return "", fmt.Errorf("file %s is empty", path)
	}
	return v, nil
}

// StaticCredentialsSource returns the three credentials it was constructed
// with on every Load. Used by the legacy HTTP-only cmd/pvc-plumber binary
// where credentials still come from secretKeyRef env vars.
type StaticCredentialsSource struct {
	creds Creds
}

// NewStaticCredentialsSource constructs a CredentialsSource that always
// returns the supplied creds. Returns ErrCredentialsNotReady on Load if
// any field is empty so test fixtures and misconfigured callers fail in
// the same way as a missing-file dir source.
func NewStaticCredentialsSource(password, accessKey, secretKey string) *StaticCredentialsSource {
	return &StaticCredentialsSource{
		creds: Creds{Password: password, AccessKey: accessKey, SecretKey: secretKey},
	}
}

func (s *StaticCredentialsSource) Load() (Creds, error) {
	if s.creds.Password == "" || s.creds.AccessKey == "" || s.creds.SecretKey == "" {
		return Creds{}, fmt.Errorf("%w: static credentials missing one or more fields", ErrCredentialsNotReady)
	}
	return s.creds, nil
}

// RepoConfig bundles the static (non-credential) inputs the restic
// subprocess needs to talk to the shared repo. Unlike kopia.S3Config there
// is no endpoint/bucket/disable-tls triple — restic encodes all of that
// into the RESTIC_REPOSITORY URL (e.g.
// `s3:https://garage.lab.mainertoo.com/volsync-shared/restic`).
type RepoConfig struct {
	Repository string
	// CacheDir overrides restic's default cache location
	// ($XDG_CACHE_HOME/restic). Set when the deployment runs with
	// readOnlyRootFilesystem and mounts an emptyDir for cache; empty
	// value lets restic pick its own location (which fails under
	// runAsNonRoot=1000 against the default `/.cache` — seen during
	// Phase 1 smoketest, recorded in the volsync label-driven project
	// memory).
	CacheDir string
}

// Client wraps the restic CLI for backup-existence checks against a
// shared S3-backed restic repository. Lazy-loads credentials from a
// CredentialsSource on every subprocess invocation, so a Secret update
// via ESO is observed without a pod restart and a Secret that hasn't
// rendered yet doesn't crash the pod at startup.
type Client struct {
	cfg            RepoConfig
	creds          CredentialsSource
	connectTimeout time.Duration
	logger         *slog.Logger
	executor       CommandExecutor

	// connected is set true once Connect() has succeeded. HealthCheck
	// requires this before it spawns `restic cat config` — there's no
	// point probing the repo over a never-validated client.
	mu        sync.RWMutex
	connected bool
}

// Options bundles the optional knobs NewClient accepts so the constructor
// surface stays small as we add more (connect timeout, future health-check
// timeout, …). All fields have sane defaults; supply zero values to keep
// them.
type Options struct {
	// ConnectTimeout caps the total time Connect() spends retrying on
	// ErrCredentialsNotReady. Defaults to 60s when zero. After this
	// elapses without seeing ready credentials, Connect returns an
	// error and the caller (controller-runtime) is expected to re-queue.
	ConnectTimeout time.Duration
}

// NewClient creates a new restic client. creds may be nil for tests that
// stub the executor and never reach a real probe; production callers MUST
// supply one.
func NewClient(cfg RepoConfig, creds CredentialsSource, logger *slog.Logger, opts Options) *Client {
	connectTimeout := opts.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 60 * time.Second
	}
	return &Client{
		cfg:            cfg,
		creds:          creds,
		connectTimeout: connectTimeout,
		logger:         logger,
		executor:       &RealExecutor{},
	}
}

// NewClientWithExecutor creates a new restic client with a custom executor
// (for testing). Mirrors NewClient's argument order — same opts shape.
func NewClientWithExecutor(cfg RepoConfig, creds CredentialsSource, logger *slog.Logger, executor CommandExecutor, opts Options) *Client {
	c := NewClient(cfg, creds, logger, opts)
	c.executor = executor
	return c
}

// envFor constructs the env slice for a restic subprocess. Always returns
// a fresh slice (caller may mutate). PATH and HOME pass through from the
// parent process so the restic binary itself and any HOME-relative cache
// fallback resolve normally; everything else is hermetic.
func (c *Client) envFor(creds Creds) []string {
	env := []string{
		"RESTIC_REPOSITORY=" + c.cfg.Repository,
		"RESTIC_PASSWORD=" + creds.Password,
		"AWS_ACCESS_KEY_ID=" + creds.AccessKey,
		"AWS_SECRET_ACCESS_KEY=" + creds.SecretKey,
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}
	if c.cfg.CacheDir != "" {
		env = append(env, "RESTIC_CACHE_DIR="+c.cfg.CacheDir)
	}
	return env
}

// Connect probes the restic repository: load credentials (retrying on
// ErrCredentialsNotReady up to connectTimeout) and issue a cheap
// `restic cat config` call to confirm the repo is reachable and the
// password is correct. Sets connected=true on success.
//
// Unlike kopia, restic has no persistent on-disk session — each command
// takes its repo URL + password via env vars and runs hermetically.
// Connect here is therefore a one-shot validation rather than a
// state-establishing operation; the connected flag exists only so
// HealthCheck has a "did startup succeed at some point" gate to match the
// kopia client's contract.
func (c *Client) Connect(ctx context.Context) error {
	deadline := time.Now().Add(c.connectTimeout)
	backoff := 250 * time.Millisecond
	const maxBackoff = 5 * time.Second

	attempt := 0
	for {
		attempt++
		creds, err := c.creds.Load()
		if err != nil {
			if !errors.Is(err, ErrCredentialsNotReady) {
				return fmt.Errorf("load restic credentials: %w", err)
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("restic credentials still not ready after %s: %w", c.connectTimeout, err)
			}
			c.logger.Warn("restic credentials not ready, retrying",
				"attempt", attempt,
				"backoff", backoff,
				"error", err,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.logger.Info("probing restic repository",
			"repository", c.cfg.Repository,
			"attempt", attempt,
		)

		env := c.envFor(creds)
		output, err := c.executor.Run(ctx, env, "restic", resticCmdCat, "config")
		if err != nil {
			c.logger.Error("failed to probe restic repository", "error", err, "output", string(output))
			return fmt.Errorf("failed to probe restic repository: %w", err)
		}

		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
		c.logger.Info("connected to restic repository")
		return nil
	}
}

// CheckBackupExists queries the shared restic repo for snapshots tagged
// `<namespace>/<pvc>`. The tag schema is part of the volsync-storage-
// recovery design — every VolSync mover Job writes this tag on each
// snapshot, so the existence of even one snapshot with this tag means the
// PVC has a recoverable backup.
//
// Returns DecisionRestore when at least one snapshot exists, DecisionFresh
// when the snapshot list is empty, and DecisionUnknown on subprocess error
// or JSON parse failure — same authoritative-flag semantics as the kopia
// backend so the webhook decision flow doesn't need to differentiate.
//
// Unlike kopia, each call reloads credentials. Restic has no on-disk
// session so there's no Connect-then-reuse pattern; the cost is one stat+
// read per /exists, which the caching layer in front of this client
// collapses to at most one subprocess call per (ns/pvc) per CACHE_TTL.
func (c *Client) CheckBackupExists(ctx context.Context, namespace, pvc string) backend.CheckResult {
	tag := namespace + "/" + pvc

	c.logger.Debug("checking restic snapshot", "tag", tag)

	creds, err := c.creds.Load()
	if err != nil {
		c.logger.Error("failed to load restic credentials for snapshot check", "error", err)
		return backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       backend.TypeResticS3,
			Source:        tag,
			Error:         fmt.Sprintf("load credentials: %v", err),
		}
	}

	env := c.envFor(creds)
	output, err := c.executor.Run(ctx, env, "restic", resticCmdSnapshots, "--tag", tag, "--latest", "1", "--json")
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			c.logger.Error("restic snapshots failed",
				"tag", tag,
				"error", err,
				"stderr", string(exitErr.Stderr))
		}
		return backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       backend.TypeResticS3,
			Source:        tag,
			Error:         fmt.Sprintf("failed to list snapshots: %v", err),
		}
	}

	var snapshots []any
	if err := json.Unmarshal(output, &snapshots); err != nil {
		c.logger.Error("failed to parse restic output", "error", err, "output", string(output))
		return backend.CheckResult{
			Exists:        false,
			Decision:      backend.DecisionUnknown,
			Authoritative: false,
			Namespace:     namespace,
			Pvc:           pvc,
			Backend:       backend.TypeResticS3,
			Source:        tag,
			Error:         fmt.Sprintf("failed to parse restic output: %v", err),
		}
	}

	exists := len(snapshots) > 0
	decision := backend.DecisionFresh
	if exists {
		decision = backend.DecisionRestore
	}
	c.logger.Debug("restic snapshot check complete", "tag", tag, "exists", exists, "count", len(snapshots))

	return backend.CheckResult{
		Exists:        exists,
		Decision:      decision,
		Authoritative: true,
		Namespace:     namespace,
		Pvc:           pvc,
		Backend:       backend.TypeResticS3,
		Source:        tag,
	}
}

// snapshotEntry represents the subset of restic snapshot JSON we care
// about. `restic snapshots --json` returns an array of objects; we only
// need tags to reconstruct the namespace/pvc set for cache prewarm.
type snapshotEntry struct {
	Tags []string `json:"tags"`
}

// ListAllSources runs `restic snapshots --json` against the shared repo
// and extracts the unique set of namespace/pvc pairs from snapshot tags.
// The tag schema is `<namespace>/<pvc>` (one or more per snapshot — the
// operator may write additional free-form tags for retention class etc.,
// which this function filters out via looksLikeNsPvcTag).
//
// Used at startup to pre-warm the cache so the first /exists call after a
// pod restart doesn't pay a subprocess round-trip.
func (c *Client) ListAllSources(ctx context.Context) (map[string]bool, error) {
	c.logger.Info("listing all restic snapshots for cache pre-warm")

	creds, err := c.creds.Load()
	if err != nil {
		return nil, fmt.Errorf("load restic credentials: %w", err)
	}
	env := c.envFor(creds)
	output, err := c.executor.Run(ctx, env, "restic", resticCmdSnapshots, "--json")
	if err != nil {
		return nil, fmt.Errorf("failed to list all snapshots: %w", err)
	}

	var entries []snapshotEntry
	if err := json.Unmarshal(output, &entries); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot list: %w", err)
	}

	sources := make(map[string]bool)
	for _, e := range entries {
		for _, tag := range e.Tags {
			if !looksLikeNsPvcTag(tag) {
				continue
			}
			sources[tag] = true
		}
	}

	c.logger.Info("snapshot scan complete", "unique_sources", len(sources))
	return sources, nil
}

// looksLikeNsPvcTag returns true iff tag is the shape `<ns>/<pvc>` —
// exactly one slash, both halves non-empty. Used to filter out free-form
// tags (retention classes, smoketest markers) from the cache prewarm map
// so the cache doesn't get polluted by tags that aren't admission keys.
func looksLikeNsPvcTag(tag string) bool {
	i := strings.IndexByte(tag, '/')
	if i <= 0 || i == len(tag)-1 {
		return false
	}
	if strings.IndexByte(tag[i+1:], '/') >= 0 {
		return false
	}
	return true
}

// IsConnected returns whether the client has successfully validated
// repository access at least once.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// HealthCheck verifies the restic repository is reachable. Used by the
// readiness probe so /readyz reflects "the restic connection is genuinely
// usable right now" rather than "the process started up successfully at
// some point in the past". Kubelet won't route admission webhook traffic
// to a not-Ready pod, so this gates failurePolicy=Fail PVC webhooks
// against a pod whose repo access has silently broken (creds rotated, S3
// endpoint unreachable, …).
//
// `restic cat config` is the cheapest call that proves repo access —
// reads exactly one object (the encrypted config blob), doesn't list
// snapshots, doesn't touch the cache. Bounded by a 5s timeout independent
// of the caller's ctx so a wedged endpoint can't pin the readiness path
// past the kubelet probe budget.
func (c *Client) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	connected := c.connected
	c.mu.RUnlock()
	if !connected {
		return fmt.Errorf("restic repository not connected")
	}

	const statusTimeout = 5 * time.Second
	probeCtx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()

	creds, err := c.creds.Load()
	if err != nil {
		return fmt.Errorf("load restic credentials: %w", err)
	}
	env := c.envFor(creds)

	if _, err := c.executor.Run(probeCtx, env, "restic", resticCmdCat, "config"); err != nil {
		return fmt.Errorf("restic cat config: %w", err)
	}
	return nil
}
