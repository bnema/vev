package ports

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// RemoteJobKind identifies one bounded unit of remote infrastructure work.
// Jobs and results are typed tagged values with request identity, not
// callbacks or raw frames.
type RemoteJobKind uint8

const (
	RemoteJobRegistryRead RemoteJobKind = iota + 1
	RemoteJobCacheLoad
	RemoteJobCatalogObserve
	RemoteJobCacheStore
)

// String returns the stable lowercase token for a job kind.
func (k RemoteJobKind) String() string {
	switch k {
	case RemoteJobRegistryRead:
		return "registry_read"
	case RemoteJobCacheLoad:
		return "cache_load"
	case RemoteJobCatalogObserve:
		return "catalog_observe"
	case RemoteJobCacheStore:
		return "cache_store"
	default:
		return "unknown"
	}
}

// RemoteJob is one admitted unit of remote work. Registration and Generation
// fence the job: results bound to a superseded registration never apply.
// Deadline bounds catalogue observation; Entries carries cache-store payload.
type RemoteJob struct {
	Kind         RemoteJobKind
	Endpoint     string
	Registration domain.RemoteRegistration
	Generation   domain.RemoteGeneration
	Deadline     time.Duration
	Entries      []catalogue.RemoteCatalogCacheEntry
}

// RemoteJobResult is exactly one completion for an admitted job: success,
// failure or cancellation. Exactly one result retires one admission credit.
// FailureKind carries the runtime's sanitized classification; Err preserves
// the cause for errors.Is/As callers.
type RemoteJobResult struct {
	Kind          RemoteJobKind
	Endpoint      string
	Registration  domain.RemoteRegistration
	Generation    domain.RemoteGeneration
	FailureKind   domain.RemoteFailureKind
	Catalog       catalogue.RemoteCatalog
	Registrations []domain.RemoteRegistration
	Entries       []catalogue.RemoteCatalogCacheEntry
	Err           error
}

// RemoteRuntime is the bounded infrastructure seam executing remote jobs. It
// owns subprocesses, storage calls and per-lane dispatch; workers never call
// into the use case. A successful job-channel send transfers exactly one
// admission credit until exactly one completion. Dispatch never blocks
// forwarding into an occupied lane; cancelled completions select on context
// cancellation.
type RemoteRuntime interface {
	Run(ctx context.Context, jobs <-chan RemoteJob, results chan<- RemoteJobResult) error
}
