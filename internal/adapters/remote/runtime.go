package remote

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	appports "github.com/bnema/vev/internal/ports"
)

const (
	// observeWorkerCount bounds concurrent catalogue observations so four
	// hung checks can never starve the registry, cache, or store lanes.
	observeWorkerCount = 4
	// defaultObserveTimeout bounds one catalogue observation when the
	// admitted job carries no deadline.
	defaultObserveTimeout = 10 * time.Second
)

// errRemoteLaneBusy retires a job the service admitted without credit. The
// service never issues more jobs of a kind than its credits; this is
// defense-in-depth so a wedged admission cannot wedge the runtime.
var errRemoteLaneBusy = errors.New("remote runtime: bounded lane occupied")

// Runtime is the bounded infrastructure seam executing remote jobs. It owns
// subprocesses, storage calls and per-lane dispatch; its workers never call
// into the use case. Lanes are independent: one registry reader, one startup
// cache reader, one cache writer, four catalogue observers.
type Runtime struct {
	store   appports.RemoteHostStore
	catalog appports.RemoteCatalogClient
	cache   appports.RemoteCatalogCache
	log     *slog.Logger
}

// NewRuntime composes a Runtime from the existing registry, catalogue and
// cache adapters. All arguments must be non-nil.
func NewRuntime(store appports.RemoteHostStore, catalog appports.RemoteCatalogClient, cache appports.RemoteCatalogCache, log *slog.Logger) *Runtime {
	if log == nil {
		log = slog.Default()
	}
	return &Runtime{store: store, catalog: catalog, cache: cache, log: log}
}

var _ appports.RemoteRuntime = (*Runtime)(nil)

// Run executes admitted jobs until ctx is cancelled or the job channel
// closes. Dispatch never blocks forwarding into an occupied lane; cancelled
// completions select on context cancellation. It returns nil on cooperative
// shutdown; lane failures travel through results, never through Run.
// Cancellation is owned explicitly: workers run under a derived context so
// closing the job channel retires in-flight delivery and reaps every worker
// instead of hanging behind idle lanes with a live parent context.
func (r *Runtime) Run(ctx context.Context, jobs <-chan appports.RemoteJob, results chan<- appports.RemoteJobResult) error {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	registryLane := make(chan appports.RemoteJob, 1)
	cacheLoadLane := make(chan appports.RemoteJob, 1)
	storeLane := make(chan appports.RemoteJob, 1)
	// The observe buffer matches the worker count: under correct service
	// credits forwarding never blocks, and scheduling jitter between rapid
	// admissions cannot retire a properly admitted job as lane-busy.
	observeLane := make(chan appports.RemoteJob, observeWorkerCount)

	var workers sync.WaitGroup
	ready := make(chan struct{}, observeWorkerCount+3)
	park := func(lane <-chan appports.RemoteJob, work func(appports.RemoteJob)) {
		defer workers.Done()
		ready <- struct{}{}
		for {
			select {
			case <-runCtx.Done():
				return
			case job, ok := <-lane:
				if !ok {
					return
				}
				work(job)
			}
		}
	}
	workers.Add(1)
	go park(registryLane, func(job appports.RemoteJob) {
		pinned, learned, err := r.store.Hosts()
		r.complete(runCtx, results, appports.RemoteJobResult{
			Kind: job.Kind, Endpoint: job.Endpoint,
			Registration: job.Registration, Generation: job.Generation,
			Registrations: combineRegistrations(pinned, learned), Err: err,
		})
	})
	workers.Add(1)
	go park(cacheLoadLane, func(job appports.RemoteJob) {
		entries, err := r.cache.Load()
		r.complete(runCtx, results, appports.RemoteJobResult{
			Kind: job.Kind, Endpoint: job.Endpoint,
			Registration: job.Registration, Generation: job.Generation,
			Entries: entries, Err: err,
		})
	})
	workers.Add(1)
	go park(storeLane, func(job appports.RemoteJob) {
		err := r.cache.Store(job.Entries)
		r.complete(runCtx, results, appports.RemoteJobResult{
			Kind: job.Kind, Endpoint: job.Endpoint,
			Registration: job.Registration, Generation: job.Generation,
			Err: err,
		})
	})
	for i := 0; i < observeWorkerCount; i++ {
		workers.Add(1)
		go park(observeLane, func(job appports.RemoteJob) {
			timeout := job.Deadline
			if timeout <= 0 {
				timeout = defaultObserveTimeout
			}
			observeCtx, cancel := context.WithTimeout(runCtx, timeout)
			catalog, err := r.catalog.List(observeCtx, job.Endpoint)
			cancel()
			r.complete(runCtx, results, appports.RemoteJobResult{
				Kind: job.Kind, Endpoint: job.Endpoint,
				Registration: job.Registration, Generation: job.Generation,
				FailureKind: ClassifyCatalogError(err), Catalog: catalog, Err: err,
			})
		})
	}
	// All lanes parked before the first admission: an immediate send can
	// never retire as lane-busy for lack of a ready receiver.
	for i := 0; i < observeWorkerCount+3; i++ {
		select {
		case <-ready:
		case <-ctx.Done():
			workers.Wait()
			return nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			workers.Wait()
			return nil
		case job, ok := <-jobs:
			if !ok {
				stop()
				workers.Wait()
				return nil
			}
			var lane chan appports.RemoteJob
			switch job.Kind {
			case appports.RemoteJobRegistryRead:
				lane = registryLane
			case appports.RemoteJobCacheLoad:
				lane = cacheLoadLane
			case appports.RemoteJobCacheStore:
				lane = storeLane
			case appports.RemoteJobCatalogObserve:
				lane = observeLane
			default:
				r.complete(runCtx, results, appports.RemoteJobResult{
					Kind: job.Kind, Endpoint: job.Endpoint,
					Registration: job.Registration, Generation: job.Generation,
					Err: errors.New("remote runtime: unknown job kind"),
				})
				continue
			}
			select {
			case lane <- job:
			case <-ctx.Done():
				workers.Wait()
				return nil
			default:
				r.log.Debug("remote runtime lane occupied, retiring job", "kind", job.Kind.String(), "endpoint", job.Endpoint)
				r.complete(runCtx, results, appports.RemoteJobResult{
					Kind: job.Kind, Endpoint: job.Endpoint,
					Registration: job.Registration, Generation: job.Generation,
					Err: errRemoteLaneBusy,
				})
			}
		}
	}
}

// complete delivers exactly one completion, abandoning delivery when the
// service is already gone so shutdown never wedges on an unread result.
func (r *Runtime) complete(ctx context.Context, results chan<- appports.RemoteJobResult, result appports.RemoteJobResult) {
	select {
	case results <- result:
	case <-ctx.Done():
	}
}

// combineRegistrations orders pinned records first in stored order, then
// learned-only records, so the monitor can assign stable presentation ranks.
func combineRegistrations(pinned, learned []domain.RemoteRegistration) []domain.RemoteRegistration {
	out := make([]domain.RemoteRegistration, 0, len(pinned)+len(learned))
	seen := make(map[string]struct{}, len(pinned)+len(learned))
	for _, record := range pinned {
		if _, dup := seen[record.Endpoint]; dup {
			continue
		}
		seen[record.Endpoint] = struct{}{}
		out = append(out, record)
	}
	for _, record := range learned {
		if _, dup := seen[record.Endpoint]; dup {
			continue
		}
		seen[record.Endpoint] = struct{}{}
		out = append(out, record)
	}
	return out
}
