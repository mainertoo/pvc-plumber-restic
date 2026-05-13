package backend

import "context"

const (
	DecisionRestore = "restore"
	DecisionFresh   = "fresh"
	DecisionUnknown = "unknown"
)

// SourceLister is the cache-prewarm contract — any backend that can
// enumerate `<ns>/<pvc>` keys implements this. Both kopia.Client and
// restic.Client satisfy it; the s3 backend does not. The re-warm loop
// in cmd/operator and cmd/pvc-plumber takes a SourceLister rather than
// a typed backend client so the same loop drives every backend that
// supports listing.
type SourceLister interface {
	ListAllSources(ctx context.Context) (map[string]bool, error)
}

// Backend type identifiers used in CheckResult.Backend and as the value of
// the BACKEND_TYPE env var that selects the runtime backend implementation.
//
// As of v3.0.0 the kopia backend connects via S3 (RustFS) instead of a local
// filesystem mount. The token name therefore changes from `kopia-fs` →
// `kopia-s3`. This is a breaking rename for anyone who scrapes
// `pvc_plumber_backup_check_total{backend="…"}` metrics or filters logs by
// backend label — see CHANGELOG v3.0.0 for the migration guidance.
const (
	TypeS3       = "s3"
	TypeKopiaS3  = "kopia-s3"
	TypeResticS3 = "restic-s3"
)

// CheckResult represents the result of a backup existence check.
type CheckResult struct {
	Exists        bool   `json:"exists"`
	Decision      string `json:"decision"`
	Authoritative bool   `json:"authoritative"`
	Namespace     string `json:"namespace"`
	Pvc           string `json:"pvc"`
	Backend       string `json:"backend"`
	Source        string `json:"source,omitempty"`
	Error         string `json:"error,omitempty"`
}
