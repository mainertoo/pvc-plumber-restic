package restic

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mitchross/pvc-plumber/internal/backend"
)

const (
	testPassword  = "testpass"
	testAccessKey = "test-access-key"
	testSecretKey = "test-secret-key"

	testRepository = "s3:https://garage.lab.example/volsync-shared/restic"

	// restic CLI subcommand literals appearing in test-side argv
	// assertions — promoted because goconst flags >=3 occurrences.
	cliSnapshots = "snapshots"
)

// testRepoConfig is the canonical repo config used by every test.
func testRepoConfig() RepoConfig {
	return RepoConfig{
		Repository: testRepository,
	}
}

// testCreds returns a StaticCredentialsSource that always loads cleanly.
func testCreds() CredentialsSource {
	return NewStaticCredentialsSource(testPassword, testAccessKey, testSecretKey)
}

// mockExecutor implements CommandExecutor for testing. Captures the most
// recent name + args + env so assertions can pin the subprocess shape.
type mockExecutor struct {
	output   []byte
	err      error
	lastName string
	lastArgs []string
	lastEnv  []string

	callCount atomic.Int64
}

func (m *mockExecutor) Run(_ context.Context, env []string, name string, args ...string) ([]byte, error) {
	m.callCount.Add(1)
	m.lastName = name
	m.lastArgs = append([]string(nil), args...)
	m.lastEnv = append([]string(nil), env...)
	return m.output, m.err
}

// envHas returns true if the captured env contains the named variable
// with the expected value. Used to pin RESTIC_REPOSITORY / RESTIC_PASSWORD
// / AWS_* without depending on slice order.
func (m *mockExecutor) envHas(key, want string) bool {
	prefix := key + "="
	for _, kv := range m.lastEnv {
		if len(kv) > len(prefix) && kv[:len(prefix)] == prefix {
			return kv[len(prefix):] == want
		}
	}
	return false
}

func TestNewClient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	client := NewClient(testRepoConfig(), testCreds(), logger, Options{})

	if client == nil {
		t.Fatal("NewClient returned nil")
	}
	if client.cfg.Repository != testRepository {
		t.Errorf("cfg.Repository = %v, want %v", client.cfg.Repository, testRepository)
	}
	if client.connected {
		t.Error("client should not be connected initially")
	}
	if client.connectTimeout != 60*time.Second {
		t.Errorf("connectTimeout default = %v, want 60s", client.connectTimeout)
	}
}

func TestNewClient_OptionsConnectTimeoutOverride(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(testRepoConfig(), testCreds(), logger, Options{ConnectTimeout: 5 * time.Second})
	if client.connectTimeout != 5*time.Second {
		t.Errorf("connectTimeout = %v, want 5s", client.connectTimeout)
	}
}

// TestNewClient_OptionsHealthCheckTimeout pins the new HealthCheckTimeout
// knob (issue #1) — zero defaults to 15s, explicit values pass through.
func TestNewClient_OptionsHealthCheckTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	def := NewClient(testRepoConfig(), testCreds(), logger, Options{})
	if def.healthCheckTimeout != 15*time.Second {
		t.Errorf("default healthCheckTimeout = %v, want 15s", def.healthCheckTimeout)
	}

	override := NewClient(testRepoConfig(), testCreds(), logger, Options{HealthCheckTimeout: 7 * time.Second})
	if override.healthCheckTimeout != 7*time.Second {
		t.Errorf("override healthCheckTimeout = %v, want 7s", override.healthCheckTimeout)
	}
}

// TestNewClient_MaxConcurrencySemaphore pins that the concurrency cap
// allocates a buffered channel sized to the option (issue #1). Zero or
// negative leaves the cap disabled (nil sem -> uncapped legacy behavior).
func TestNewClient_MaxConcurrencySemaphore(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	uncapped := NewClient(testRepoConfig(), testCreds(), logger, Options{})
	if uncapped.sem != nil {
		t.Errorf("default MaxConcurrency should leave sem nil (uncapped), got %d cap", cap(uncapped.sem))
	}

	capped := NewClient(testRepoConfig(), testCreds(), logger, Options{MaxConcurrency: 3})
	if capped.sem == nil || cap(capped.sem) != 3 {
		t.Errorf("MaxConcurrency=3 should make sem with cap 3, got %v", capped.sem)
	}
}

// gatedExecutor is a CommandExecutor whose Run blocks on `release` and
// tracks the peak number of concurrent in-flight calls. Used to prove the
// semaphore actually limits concurrency rather than just allocating the
// channel.
type gatedExecutor struct {
	release  chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32
}

func (g *gatedExecutor) Run(ctx context.Context, _ []string, _ string, _ ...string) ([]byte, error) {
	cur := g.inFlight.Add(1)
	defer g.inFlight.Add(-1)
	for {
		prev := g.peak.Load()
		if cur <= prev || g.peak.CompareAndSwap(prev, cur) {
			break
		}
	}
	select {
	case <-g.release:
		return []byte("ok"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestRunRestic_SemaphoreLimitsConcurrency pins the runtime contract for
// the new semaphore: with MaxConcurrency=2, four concurrent runRestic
// calls must serialize through at most 2 executor invocations at a time
// (issue #1). This is the test that would actually fail if someone broke
// the semaphore (e.g., dropped the release).
func TestRunRestic_SemaphoreLimitsConcurrency(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	gate := &gatedExecutor{release: make(chan struct{})}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, gate, Options{MaxConcurrency: 2})

	const callers = 4
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_, _ = client.runRestic(context.Background(), nil, "restic", "snapshots")
		}()
	}

	// Give all callers time to either reach the executor or block on the
	// semaphore. 100ms is generous; the test is robust at 25ms.
	time.Sleep(100 * time.Millisecond)

	if got := gate.inFlight.Load(); got > 2 {
		t.Errorf("in-flight executor calls = %d, want <= 2 (semaphore cap)", got)
	}

	close(gate.release)
	wg.Wait()

	if got := gate.peak.Load(); got > 2 {
		t.Errorf("peak concurrent executor calls = %d, want <= 2 (semaphore cap)", got)
	}
}

// TestRunRestic_NoCapWhenMaxConcurrencyZero pins that omitting the cap
// preserves the legacy uncapped behavior — 4 concurrent calls land in the
// executor simultaneously, not serialized through any semaphore.
func TestRunRestic_NoCapWhenMaxConcurrencyZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	gate := &gatedExecutor{release: make(chan struct{})}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, gate, Options{})

	const callers = 4
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_, _ = client.runRestic(context.Background(), nil, "restic", "snapshots")
		}()
	}

	time.Sleep(100 * time.Millisecond)

	if got := gate.inFlight.Load(); got != callers {
		t.Errorf("in-flight executor calls = %d, want %d (no cap)", got, callers)
	}

	close(gate.release)
	wg.Wait()
}

// TestHealthCheck_HonorsConfiguredTimeout pins that HEALTH_CHECK_TIMEOUT
// actually bounds the readiness probe's inner restic call (issue #1).
// Sets a 50ms budget, gives the executor a 1s release horizon, expects
// the probe to return DeadlineExceeded long before the 1s elapses.
func TestHealthCheck_HonorsConfiguredTimeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	gate := &gatedExecutor{release: make(chan struct{})}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, gate, Options{HealthCheckTimeout: 50 * time.Millisecond})
	client.mu.Lock()
	client.connected = true
	client.mu.Unlock()

	start := time.Now()
	err := client.HealthCheck(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("HealthCheck should fail when restic exceeds HealthCheckTimeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("HealthCheck err = %v, want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("HealthCheck ran for %v before timing out; expected ~50ms (probe budget)", elapsed)
	}
	close(gate.release)
}

// TestConnect_Success pins the probe shape: `restic cat config` plus all
// four env vars populated. The probe must NOT pass credentials on argv
// (would leak password into ps output).
func TestConnect_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Errorf("Connect() error = %v, want nil", err)
	}
	if !client.connected {
		t.Error("client should be connected after successful Connect()")
	}
	if !client.IsConnected() {
		t.Error("IsConnected() should return true")
	}
	if mock.lastName != "restic" {
		t.Errorf("executor command = %q, want restic", mock.lastName)
	}
	wantArgs := []string{"cat", "config"}
	if !slices.Equal(mock.lastArgs, wantArgs) {
		t.Errorf("executor args = %v, want %v", mock.lastArgs, wantArgs)
	}
	// Credentials must travel in env, never in argv.
	for _, secret := range []string{testPassword, testAccessKey, testSecretKey} {
		if slices.Contains(mock.lastArgs, secret) {
			t.Errorf("credential %q leaked into argv %v", secret, mock.lastArgs)
		}
	}
	// Pin all four env vars on the captured slice.
	for key, want := range map[string]string{
		"RESTIC_REPOSITORY":     testRepository,
		"RESTIC_PASSWORD":       testPassword,
		"AWS_ACCESS_KEY_ID":     testAccessKey,
		"AWS_SECRET_ACCESS_KEY": testSecretKey,
	} {
		if !mock.envHas(key, want) {
			t.Errorf("env missing %s=%s; got %v", key, want, mock.lastEnv)
		}
	}
}

// TestConnect_PassesCacheDir pins that RESTIC_CACHE_DIR is set when
// RepoConfig.CacheDir is non-empty. The deployment mounts an emptyDir at
// /.cache (Phase 1 finding); restic must see RESTIC_CACHE_DIR pointing
// there or it'll fail under readOnlyRootFilesystem.
func TestConnect_PassesCacheDir(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := testRepoConfig()
	cfg.CacheDir = "/var/cache/restic"
	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(cfg, testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if !mock.envHas("RESTIC_CACHE_DIR", "/var/cache/restic") {
		t.Errorf("env missing RESTIC_CACHE_DIR; got %v", mock.lastEnv)
	}
}

// TestConnect_NoCacheDirWhenEmpty pins the inverse: with CacheDir empty,
// RESTIC_CACHE_DIR must not be set (restic falls back to XDG default).
func TestConnect_NoCacheDirWhenEmpty(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	for _, kv := range mock.lastEnv {
		if len(kv) >= len("RESTIC_CACHE_DIR=") && kv[:len("RESTIC_CACHE_DIR=")] == "RESTIC_CACHE_DIR=" {
			t.Errorf("env should not include RESTIC_CACHE_DIR when CacheDir is empty; got %q", kv)
		}
	}
}

func TestConnect_Failure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{
		output: []byte("repository not found"),
		err:    errors.New("exit status 1"),
	}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	err := client.Connect(context.Background())
	if err == nil {
		t.Error("Connect() should have returned an error")
	}
	if client.connected {
		t.Error("client should not be connected after failed Connect()")
	}
}

// TestConnect_RetriesOnCredentialsNotReady pins the backoff loop: when the
// credentials source returns ErrCredentialsNotReady on the first few calls
// and then succeeds, Connect() should keep retrying until creds appear
// and finally invoke the restic subprocess exactly once.
func TestConnect_RetriesOnCredentialsNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &flakyCreds{readyAfter: 3, ready: testCreds()}
	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), creds, logger, mock, Options{ConnectTimeout: 5 * time.Second})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v, want nil after creds-not-ready retries", err)
	}
	if !client.IsConnected() {
		t.Error("client should be connected after retried success")
	}
	if creds.calls != 3 {
		t.Errorf("creds.Load() call count = %d, want 3 (2 not-ready + 1 ready)", creds.calls)
	}
	if got := mock.callCount.Load(); got != 1 {
		t.Errorf("executor invocations = %d, want 1", got)
	}
}

// TestConnect_TimesOutOnPersistentNotReady pins the deadline guard.
func TestConnect_TimesOutOnPersistentNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &flakyCreds{readyAfter: 9999} // never becomes ready
	mock := &mockExecutor{output: []byte("ok"), err: nil}

	timeout := 600 * time.Millisecond
	client := NewClientWithExecutor(testRepoConfig(), creds, logger, mock, Options{ConnectTimeout: timeout})

	start := time.Now()
	err := client.Connect(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Connect() must return an error when credentials never become ready")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("error chain should include ErrCredentialsNotReady; got %v", err)
	}
	if elapsed < timeout {
		t.Errorf("Connect returned in %v, want at least the configured ConnectTimeout %v", elapsed, timeout)
	}
	if elapsed > 4*timeout {
		t.Errorf("Connect took %v, much longer than 4× ConnectTimeout %v — backoff loop runaway?", elapsed, timeout)
	}
	if client.IsConnected() {
		t.Error("client must not be marked connected after timeout")
	}
	if got := mock.callCount.Load(); got != 0 {
		t.Errorf("executor must not be invoked when creds never ready; got %d calls", got)
	}
}

// TestConnect_HardErrorPropagates pins that any error class OTHER than
// ErrCredentialsNotReady from the creds source short-circuits the retry
// loop — a permanent misconfiguration must not be silently retried for
// a full minute.
func TestConnect_HardErrorPropagates(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &errorCreds{err: errors.New("credentials malformed")}
	mock := &mockExecutor{}
	client := NewClientWithExecutor(testRepoConfig(), creds, logger, mock, Options{ConnectTimeout: 60 * time.Second})

	err := client.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect() should have returned an error")
	}
	if errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("hard creds error must NOT be wrapped as ErrCredentialsNotReady; got %v", err)
	}
}

func TestCheckBackupExists_Found(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// restic snapshots --json returns an array of snapshot objects; one
	// entry is enough to flip exists=true.
	snapshotJSON := `[
		{
			"id": "abc123",
			"time": "2024-01-15T10:00:00Z",
			"tags": ["karakeep/test-pvc"],
			"paths": ["/data"],
			"hostname": "mover-pod"
		}
	]`

	mock := &mockExecutor{output: []byte(snapshotJSON), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	result := client.CheckBackupExists(context.Background(), "karakeep", "test-pvc")

	if !result.Exists {
		t.Error("Exists should be true")
	}
	if result.Decision != backend.DecisionRestore {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionRestore)
	}
	if !result.Authoritative {
		t.Error("Authoritative should be true")
	}
	if result.Source != "karakeep/test-pvc" {
		t.Errorf("Source = %v, want karakeep/test-pvc", result.Source)
	}
	if result.Namespace != "karakeep" {
		t.Errorf("Namespace = %v, want karakeep", result.Namespace)
	}
	if result.Pvc != "test-pvc" {
		t.Errorf("Pvc = %v, want test-pvc", result.Pvc)
	}
	if result.Backend != backend.TypeResticS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeResticS3)
	}
	if result.Error != "" {
		t.Errorf("Error = %v, want empty", result.Error)
	}
	// Pin the argv shape: snapshots --tag <ns>/<pvc> --latest 1 --json.
	wantArgs := []string{cliSnapshots, "--tag", "karakeep/test-pvc", "--latest", "1", "--json"}
	if !slices.Equal(mock.lastArgs, wantArgs) {
		t.Errorf("executor args = %v, want %v", mock.lastArgs, wantArgs)
	}
}

func TestCheckBackupExists_NotFound(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("[]"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	result := client.CheckBackupExists(context.Background(), "foo", "bar")

	if result.Exists {
		t.Error("Exists should be false")
	}
	if result.Decision != backend.DecisionFresh {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionFresh)
	}
	if !result.Authoritative {
		t.Error("Authoritative should be true")
	}
	if result.Source != "foo/bar" {
		t.Errorf("Source = %v, want foo/bar", result.Source)
	}
	if result.Backend != backend.TypeResticS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeResticS3)
	}
}

func TestCheckBackupExists_CommandError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: nil, err: errors.New("command failed")}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on error")
	}
	if result.Decision != backend.DecisionUnknown {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionUnknown)
	}
	if result.Authoritative {
		t.Error("Authoritative should be false on error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on command failure")
	}
	if result.Backend != backend.TypeResticS3 {
		t.Errorf("Backend = %v, want %s", result.Backend, backend.TypeResticS3)
	}
}

func TestCheckBackupExists_InvalidJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("not valid json"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	result := client.CheckBackupExists(context.Background(), "test-ns", "test-pvc")

	if result.Exists {
		t.Error("Exists should be false on JSON parse error")
	}
	if result.Decision != backend.DecisionUnknown {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionUnknown)
	}
	if result.Authoritative {
		t.Error("Authoritative should be false on JSON parse error")
	}
	if result.Error == "" {
		t.Error("Error should not be empty on JSON parse failure")
	}
}

// TestCheckBackupExists_CredsNotReady pins the creds-fail path: when the
// credentials source errors at call time, we report Unknown with the
// error message rather than crashing or hanging.
func TestCheckBackupExists_CredsNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	creds := &errorCreds{err: ErrCredentialsNotReady}
	mock := &mockExecutor{}
	client := NewClientWithExecutor(testRepoConfig(), creds, logger, mock, Options{})

	result := client.CheckBackupExists(context.Background(), "ns", "pvc")

	if result.Decision != backend.DecisionUnknown {
		t.Errorf("Decision = %v, want %v", result.Decision, backend.DecisionUnknown)
	}
	if result.Authoritative {
		t.Error("Authoritative should be false on creds failure")
	}
	if mock.callCount.Load() != 0 {
		t.Errorf("executor must not be invoked when creds load fails; got %d calls", mock.callCount.Load())
	}
}

func TestListAllSources_extractsNsPvcTags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	snapshotJSON := `[
		{"id": "1", "tags": ["karakeep/test-pvc"]},
		{"id": "2", "tags": ["karakeep/test-pvc"]},
		{"id": "3", "tags": ["wiki-js/wiki-data"]},
		{"id": "4", "tags": ["hourly", "wiki-js/wiki-data"]}
	]`

	mock := &mockExecutor{output: []byte(snapshotJSON), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	got, err := client.ListAllSources(context.Background())
	if err != nil {
		t.Fatalf("ListAllSources() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d unique sources, want 2: %v", len(got), got)
	}
	if !got["karakeep/test-pvc"] {
		t.Errorf("expected karakeep/test-pvc in sources; got %v", got)
	}
	if !got["wiki-js/wiki-data"] {
		t.Errorf("expected wiki-js/wiki-data in sources; got %v", got)
	}
}

// TestListAllSources_ignoresNonNsPvcTags pins the tag-shape filter: free-
// form tags (retention class, smoketest markers) must NOT pollute the
// cache prewarm map.
func TestListAllSources_ignoresNonNsPvcTags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	snapshotJSON := `[
		{"id": "1", "tags": ["hourly", "phase1-smoketest", "foo/bar/baz", "/leadingslash", "trailingslash/"]},
		{"id": "2", "tags": ["valid/pvc"]}
	]`

	mock := &mockExecutor{output: []byte(snapshotJSON), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	got, err := client.ListAllSources(context.Background())
	if err != nil {
		t.Fatalf("ListAllSources() error = %v", err)
	}
	if len(got) != 1 {
		t.Errorf("got %d sources, want 1 (only valid/pvc); got %v", len(got), got)
	}
	if !got["valid/pvc"] {
		t.Errorf("expected valid/pvc; got %v", got)
	}
}

func TestLooksLikeNsPvcTag(t *testing.T) {
	cases := []struct {
		tag  string
		want bool
	}{
		{"ns/pvc", true},
		{"a/b", true},
		{"karakeep/karakeep-data-pvc", true},
		{"", false},
		{"hourly", false},
		{"/pvc", false},
		{"ns/", false},
		{"a/b/c", false},
		{"//", false},
	}
	for _, c := range cases {
		if got := looksLikeNsPvcTag(c.tag); got != c.want {
			t.Errorf("looksLikeNsPvcTag(%q) = %v, want %v", c.tag, got, c.want)
		}
	}
}

func TestHealthCheck_NotConnected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(testRepoConfig(), testCreds(), logger, Options{})

	if err := client.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck should fail when client is not connected")
	}
}

// TestHealthCheck_Success pins that once Connect() has marked the client
// connected AND `restic cat config` succeeds, the readiness probe returns
// nil. Verifies the cheap-probe selection — must be `cat config`, not
// `snapshots`.
func TestHealthCheck_Success(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	if err := client.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck() error = %v, want nil", err)
	}
	wantArgs := []string{"cat", "config"}
	if !slices.Equal(mock.lastArgs, wantArgs) {
		t.Errorf("HealthCheck must invoke `restic cat config`; got args %v", mock.lastArgs)
	}
}

// TestHealthCheck_StatusFailureFailsReadiness pins that when `restic cat
// config` errors out, HealthCheck returns non-nil so the kubelet marks
// the pod not-Ready and stops routing admission webhook traffic.
func TestHealthCheck_StatusFailureFailsReadiness(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), testCreds(), logger, mock, Options{})

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}

	statusFail := &mockExecutor{output: []byte("auth failed"), err: errors.New("exit status 1")}
	client.executor = statusFail

	if err := client.HealthCheck(context.Background()); err == nil {
		t.Error("HealthCheck() must fail when `restic cat config` errors out")
	}
}

func TestDirCredentialsSource_LoadsAllThreeFiles(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "RESTIC_PASSWORD"), "secret-pass\n")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak-123\n")
	mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk-456\n")

	src := NewDirCredentialsSource(dir)
	got, err := src.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Password != "secret-pass" {
		t.Errorf("Password = %q, want %q", got.Password, "secret-pass")
	}
	if got.AccessKey != "ak-123" {
		t.Errorf("AccessKey = %q, want %q", got.AccessKey, "ak-123")
	}
	if got.SecretKey != "sk-456" {
		t.Errorf("SecretKey = %q, want %q", got.SecretKey, "sk-456")
	}
}

func TestDirCredentialsSource_MissingFileIsNotReady(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "RESTIC_PASSWORD"), "p\n")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak\n")
	// AWS_SECRET_ACCESS_KEY missing.

	src := NewDirCredentialsSource(dir)
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error when a credential file is missing")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("missing file error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

func TestDirCredentialsSource_EmptyFileIsNotReady(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "RESTIC_PASSWORD"), "")
	mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak")
	mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk")

	src := NewDirCredentialsSource(dir)
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error on empty credential file")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("empty file error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

func TestDirCredentialsSource_EmptyDirIsNotReady(t *testing.T) {
	src := NewDirCredentialsSource("")
	_, err := src.Load()
	if err == nil {
		t.Fatal("Load() must error when Dir is empty")
	}
	if !errors.Is(err, ErrCredentialsNotReady) {
		t.Errorf("empty Dir error must wrap ErrCredentialsNotReady; got %v", err)
	}
}

func TestStaticCredentialsSource_HappyPath(t *testing.T) {
	src := NewStaticCredentialsSource("p", "ak", "sk")
	got, err := src.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != (Creds{Password: "p", AccessKey: "ak", SecretKey: "sk"}) {
		t.Errorf("Load() returned %+v, want full creds", got)
	}
}

func TestStaticCredentialsSource_EmptyFieldIsNotReady(t *testing.T) {
	cases := []struct {
		name      string
		password  string
		accessKey string
		secretKey string
	}{
		{"missing password", "", "ak", "sk"},
		{"missing access key", "p", "", "sk"},
		{"missing secret key", "p", "ak", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := NewStaticCredentialsSource(tc.password, tc.accessKey, tc.secretKey)
			_, err := src.Load()
			if err == nil {
				t.Fatal("expected error on missing field")
			}
			if !errors.Is(err, ErrCredentialsNotReady) {
				t.Errorf("error must wrap ErrCredentialsNotReady; got %v", err)
			}
		})
	}
}

// TestConnect_DirCredentialsSource_AppearsLate is the integration-y
// scenario: the cred files don't exist yet at Connect() time but appear
// shortly after, simulating ESO catching up after an ArgoCD sync wave.
func TestConnect_DirCredentialsSource_AppearsLate(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	dir := t.TempDir()

	src := NewDirCredentialsSource(dir)
	mock := &mockExecutor{output: []byte("ok"), err: nil}
	client := NewClientWithExecutor(testRepoConfig(), src, logger, mock, Options{ConnectTimeout: 5 * time.Second})

	go func() {
		time.Sleep(400 * time.Millisecond)
		mustWrite(t, filepath.Join(dir, "RESTIC_PASSWORD"), "p\n")
		mustWrite(t, filepath.Join(dir, "AWS_ACCESS_KEY_ID"), "ak\n")
		mustWrite(t, filepath.Join(dir, "AWS_SECRET_ACCESS_KEY"), "sk\n")
	}()

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v, want creds-appear-late path to succeed", err)
	}
	if !client.IsConnected() {
		t.Error("client should be connected after creds appeared")
	}
}

// flakyCreds returns ErrCredentialsNotReady for the first `readyAfter-1`
// calls, then delegates to `ready` for subsequent calls.
type flakyCreds struct {
	readyAfter int
	calls      int
	ready      CredentialsSource
}

func (f *flakyCreds) Load() (Creds, error) {
	f.calls++
	if f.calls < f.readyAfter {
		return Creds{}, ErrCredentialsNotReady
	}
	if f.ready == nil {
		return Creds{}, ErrCredentialsNotReady
	}
	return f.ready.Load()
}

// errorCreds always returns the configured error, exercising the
// hard-error-propagates branch of Connect() and CheckBackupExists.
type errorCreds struct {
	err error
}

func (e *errorCreds) Load() (Creds, error) {
	return Creds{}, e.err
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
