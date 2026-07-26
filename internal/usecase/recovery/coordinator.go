// Package recovery coordinates crash-resumable operations across the catalogue
// and snapshot repository without depending on filesystem adapters.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

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

var (
	// ErrDeletionTombstoneInvalid marks a tombstone whose own identity cannot be
	// used. ErrDeletionTombstoneConflict marks a tombstone that disagrees with a
	// live catalogue record. Both are session-scoped: they fence one session
	// instead of failing startup.
	ErrDeletionTombstoneInvalid  = errors.New("recovery: deletion tombstone is unusable")
	ErrDeletionTombstoneConflict = errors.New("recovery: deletion tombstone conflicts with catalogue")
	// ErrSessionRecordInvalid marks a catalogue record whose own contents cannot
	// be advanced deterministically.
	ErrSessionRecordInvalid = errors.New("recovery: catalogue record is unusable")
)

// Degraded reasons are content-free by construction: they never carry paths,
// terminal content, or operator input.
const (
	degradedReasonDiscardIntent = "discard intent conflict"
	degradedReasonTombstone     = "deletion tombstone conflict"
	degradedReasonRecord        = "catalogue record conflict"
)

type Coordinator struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
	journal    ports.RecoveryJournal
	locks      *KeyLocks
	random     io.Reader

	conflictMu sync.Mutex
	conflicts  []RecoveryConflict
}

// RecoveryConflict is one session-scoped item Recover fenced off. Recover keeps
// the durable source (intent or tombstone) so a later operator action can still
// resolve it; startup continues for every other session.
type RecoveryConflict struct {
	Session string
	Kind    string
	Err     error
}

// Conflicts returns the session-scoped items fenced by the most recent Recover call.
func (c *Coordinator) Conflicts() []RecoveryConflict {
	if c == nil {
		return nil
	}
	c.conflictMu.Lock()
	defer c.conflictMu.Unlock()
	return append([]RecoveryConflict(nil), c.conflicts...)
}

func NewCoordinator(catalogue ports.Catalogue, repository ports.SnapshotRepository, journal ports.RecoveryJournal, random io.Reader) *Coordinator {
	return &Coordinator{catalogue: catalogue, repository: repository, journal: journal, locks: NewKeyLocks(), random: random}
}

// Create commits fresh catalogue metadata before the caller creates or exposes
// any runtime session.
func (c *Coordinator) Create(ctx context.Context, record domain.CatalogueRecord) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.locks == nil || c.random == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete create dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	unlock := c.locks.Lock([]string{record.Name})
	defer unlock()
	if _, exists, err := c.catalogue.Record(record.Name); err != nil {
		return domain.CatalogueRecord{}, err
	} else if exists {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q already exists", record.Name)
	}
	id, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: generate incarnation: %w", err)
	}
	record.IncarnationID = id
	record.RecoveryState = domain.RecoveryFresh
	record.Committed = nil
	record.Fallbacks = [2]*domain.CheckpointRef{}
	record.DegradedReason = ""
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid fresh record: %w", err)
	}
	if err := c.catalogue.Create(record); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return record, nil
}

// Rename atomically moves one catalogue key while retaining the immutable
// incarnation and all checkpoint references.
func (c *Coordinator) Rename(ctx context.Context, oldName, newName string) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.locks == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete rename dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	unlock := c.locks.Lock([]string{oldName, newName})
	defer unlock()
	record, ok, err := c.catalogue.Record(oldName)
	if err != nil {
		return domain.CatalogueRecord{}, err
	}
	if !ok {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q not found", oldName)
	}
	if existing, exists, err := c.catalogue.Record(newName); err != nil {
		return domain.CatalogueRecord{}, err
	} else if exists && (newName != oldName || existing.IncarnationID != record.IncarnationID) {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q already exists", newName)
	}
	record.Name = newName
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid rename: %w", err)
	}
	if oldName != newName {
		if err := c.catalogue.Rename(oldName, record); err != nil {
			return domain.CatalogueRecord{}, err
		}
	}
	return record, nil
}

// Delete executes the durable deletion protocol. Every completed boundary is
// independently sufficient for Recover to roll the operation forward.
func (c *Coordinator) Delete(ctx context.Context, name string) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete delete dependencies")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return c.deleteLocked(ctx, record)
}

func (c *Coordinator) deleteLocked(ctx context.Context, record domain.CatalogueRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.RecoveryState != domain.RecoveryDeleting {
		record.RecoveryState = domain.RecoveryDeleting
		record.DegradedReason = ""
		if err := record.Validate(); err != nil {
			return fmt.Errorf("%w: invalid deleting record: %w", ErrSessionRecordInvalid, err)
		}
		if err := c.catalogue.Replace(record.Name, record); err != nil {
			return err
		}
	}
	tombstone := domain.DeletionTombstone{Name: record.Name, IncarnationID: record.IncarnationID}
	if err := c.repository.WriteDeletionTombstone(ctx, tombstone); err != nil {
		return err
	}
	if err := c.repository.QuarantineDeletionSources(ctx, tombstone, true); err != nil {
		return err
	}
	if err := c.catalogue.Delete(record.Name); err != nil {
		return err
	}
	return c.repository.DeleteDeletionTombstone(ctx, record.IncarnationID)
}

// Recover rolls deleting records and strict deletion tombstones forward. The
// two sources are enumerated independently because catalogue removal precedes
// tombstone removal in the commit protocol.
//
// A per-item failure that only proves one session's durable state disagrees
// with itself fences that session and lets startup continue: healthy sessions
// must still restore next to a corrupt neighbour. Infrastructure failures
// (listing, journal, repository, or catalogue IO, and a non-advancing cursor)
// still abort startup, because they say nothing about any single session.
func (c *Coordinator) Recover(ctx context.Context) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.journal == nil || c.locks == nil {
		return errors.New("recovery: incomplete coordinator dependencies")
	}
	c.conflictMu.Lock()
	c.conflicts = nil
	c.conflictMu.Unlock()

	intents, err := c.journal.ListDiscards(ctx)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		// The intent is deliberately retained for a fenced session: only a
		// successful roll-forward may remove it.
		if err := c.recoverDiscard(ctx, intent); err != nil {
			if err := c.fenceSession(intent.SessionName, "discard-intent", degradedReasonDiscardIntent, err); err != nil {
				return err
			}
		}
	}
	records, err := c.catalogue.Records()
	if err != nil {
		return err
	}
	for _, candidate := range records {
		if candidate.RecoveryState != domain.RecoveryDeleting {
			continue
		}
		unlock := c.locks.Lock([]string{candidate.Name})
		var deleteErr error
		current, ok, readErr := c.catalogue.Record(candidate.Name)
		if readErr != nil {
			unlock()
			return readErr
		}
		if ok && current.IncarnationID == candidate.IncarnationID && current.RecoveryState == domain.RecoveryDeleting {
			deleteErr = c.deleteLocked(ctx, current)
		}
		unlock()
		if deleteErr != nil {
			if err := c.fenceSession(candidate.Name, "catalogue-record", degradedReasonRecord, deleteErr); err != nil {
				return err
			}
		}
	}

	cursor := ports.DeletionTombstoneCursor{}
	for {
		page, err := c.repository.ListDeletionTombstones(ctx, cursor, recoveryListingBudget)
		if err != nil {
			return err
		}
		for _, tombstone := range page.Tombstones {
			// A fenced tombstone is never deleted here: the next startup or an
			// explicit operator action must still be able to observe it.
			if err := c.recoverDeletionTombstone(ctx, tombstone); err != nil {
				if err := c.fenceSession(tombstone.Name, "deletion-tombstone", degradedReasonTombstone, err); err != nil {
					return err
				}
			}
		}
		if page.Done {
			return nil
		}
		if page.Next.After == cursor.After {
			return errors.New("recovery: tombstone listing did not advance")
		}
		cursor = page.Next
	}
}

// fenceSession returns cause unchanged when it is not session-scoped, so the
// caller aborts startup. For a session-scoped cause it records the conflict and
// durably marks the session degraded when the record can represent that state.
func (c *Coordinator) fenceSession(name, kind, reason string, cause error) error {
	if !isSessionScopedConflict(cause) {
		return cause
	}
	c.conflictMu.Lock()
	c.conflicts = append(c.conflicts, RecoveryConflict{Session: name, Kind: kind, Err: cause})
	c.conflictMu.Unlock()

	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok || record.RecoveryState != domain.RecoveryHealthy {
		// Fresh records cannot represent degraded state (it requires a committed
		// checkpoint), and deleting or already degraded records must keep the
		// state their own protocol owns. Fencing is then the recorded skip.
		return nil
	}
	record.RecoveryState = domain.RecoveryDegraded
	record.DegradedReason = reason
	if err := record.Validate(); err != nil {
		return nil
	}
	// A catalogue write failure here is infrastructure, not one session's data.
	return c.catalogue.Replace(name, record)
}

// isSessionScopedConflict reports whether err proves only that one session's
// durable state is self-inconsistent. Everything else - IO, decode, traversal,
// context cancellation - is treated as infrastructure and aborts startup.
func isSessionScopedConflict(err error) bool {
	return errors.Is(err, ErrDiscardConflict) || errors.Is(err, ErrDiscardIntentInvalid) ||
		errors.Is(err, ErrDeletionTombstoneConflict) || errors.Is(err, ErrDeletionTombstoneInvalid) ||
		errors.Is(err, ErrSessionRecordInvalid)
}

func (c *Coordinator) recoverDeletionTombstone(ctx context.Context, tombstone domain.DeletionTombstone) error {
	if err := tombstone.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrDeletionTombstoneInvalid, err)
	}
	unlock := c.locks.Lock([]string{tombstone.Name})
	defer unlock()
	record, exists, err := c.catalogue.Record(tombstone.Name)
	if err != nil {
		return err
	}
	includeLegacyName := true
	if exists {
		switch {
		case record.IncarnationID != tombstone.IncarnationID:
			includeLegacyName = false
		case record.RecoveryState != domain.RecoveryDeleting:
			return fmt.Errorf("%w: non-deleting session %q", ErrDeletionTombstoneConflict, tombstone.Name)
		}
	}
	if err := c.repository.QuarantineDeletionSources(ctx, tombstone, includeLegacyName); err != nil {
		return err
	}
	if exists && record.IncarnationID == tombstone.IncarnationID {
		if err := c.catalogue.Delete(tombstone.Name); err != nil {
			return err
		}
	}
	return c.repository.DeleteDeletionTombstone(ctx, tombstone.IncarnationID)
}
