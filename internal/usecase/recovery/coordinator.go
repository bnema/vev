// Package recovery coordinates crash-resumable operations across the catalogue
// and snapshot repository without depending on filesystem adapters.
package recovery

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

var ErrPendingRecoveryUnsupported = errors.New("pending recovery intent requires transaction recovery")

var recoveryListingBudget = ports.MaintenanceBudget{Entries: 64, Bytes: 64 << 10}

type KeyLocks struct {
	mu   sync.Mutex
	refs map[string]*keyLockRef
}

type keyLockRef struct {
	mu   sync.Mutex
	refs int
}

func NewKeyLocks() *KeyLocks {
	return &KeyLocks{refs: make(map[string]*keyLockRef)}
}

// Lock acquires each distinct stable key in lexical order. The returned
// function releases in reverse order and removes unused lock entries.
func (k *KeyLocks) Lock(names []string) func() {
	if k == nil {
		return func() {}
	}
	ordered := append([]string(nil), names...)
	sort.Strings(ordered)
	ordered = compactStrings(ordered)

	refs := make([]*keyLockRef, len(ordered))
	k.mu.Lock()
	for i, name := range ordered {
		ref := k.refs[name]
		if ref == nil {
			ref = &keyLockRef{}
			k.refs[name] = ref
		}
		ref.refs++
		refs[i] = ref
	}
	k.mu.Unlock()
	for _, ref := range refs {
		ref.mu.Lock()
	}
	return func() {
		for i := len(refs) - 1; i >= 0; i-- {
			refs[i].mu.Unlock()
		}
		k.mu.Lock()
		for i, name := range ordered {
			refs[i].refs--
			if refs[i].refs == 0 {
				delete(k.refs, name)
			}
		}
		k.mu.Unlock()
	}
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

type Coordinator struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
	journal    ports.RecoveryJournal
	locks      *KeyLocks
	random     io.Reader
}

func NewCoordinator(catalogue ports.Catalogue, repository ports.SnapshotRepository, journal ports.RecoveryJournal, random io.Reader) *Coordinator {
	return &Coordinator{catalogue: catalogue, repository: repository, journal: journal, locks: NewKeyLocks(), random: random}
}

// Recover is deliberately fail-closed until the transaction roll-forward
// handlers are introduced. Startup may proceed only when both intent sources
// have been completely and strictly enumerated.
func (c *Coordinator) Recover(ctx context.Context) error {
	if c == nil || c.repository == nil || c.journal == nil {
		return errors.New("recovery: incomplete coordinator dependencies")
	}
	intents, err := c.journal.ListDiscards(ctx)
	if err != nil {
		return err
	}
	pending := len(intents) != 0
	cursor := ports.DeletionTombstoneCursor{}
	for {
		page, err := c.repository.ListDeletionTombstones(ctx, cursor, recoveryListingBudget)
		if err != nil {
			return err
		}
		if len(page.Tombstones) != 0 {
			pending = true
		}
		if page.Done {
			break
		}
		if page.Next.After == cursor.After {
			return errors.New("recovery: tombstone listing did not advance")
		}
		cursor = page.Next
	}
	if pending {
		return ErrPendingRecoveryUnsupported
	}
	return nil
}
