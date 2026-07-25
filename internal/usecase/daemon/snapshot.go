package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/vt"
)

// snapshotQueueCapacity bounds retained immutable captures. A full queue never
// stalls session producers; the session remains dirty for the next interval.
const snapshotQueueCapacity = 1

// snapshotFinalQueueCapacity is a global bound for terminal captures retained
// while the worker is blocked. On saturation a new terminal capture is rejected
// (and left dirty) rather than silently dropping a persistence acknowledgement.
const snapshotFinalQueueCapacity = 32

// snapshotAttemptKind distinguishes rate-limited routine publications from
// mandatory terminal checkpoints. Forced attempts never consume the routine
// completion window.
type snapshotAttemptKind uint8

const (
	snapshotAttemptRoutine snapshotAttemptKind = iota
	snapshotAttemptForced
)

type snapshotCapture struct {
	session              *session
	attemptKind          snapshotAttemptKind
	generation           uint64 // repository publication generation
	parentCheckpoint     *domain.CheckpointRef
	checkpoint           domain.CheckpointRef // set by the encoder before publication
	mutationRevision     uint64
	name                 string
	incarnation          domain.IncarnationID
	createdAt            uint64
	active               uint16
	tabs                 []snapshotCaptureTab
	publicationContext   context.Context
	sealedRefs           map[*vt.HistoryChunk]snapcodec.ObjectRef // set by the single encoder worker
	coordinatorDiscarded bool                                     // guarded by session.snapshotMu
	finishOnce           sync.Once
}

type snapshotCaptureTab struct {
	stableID   string
	cols       uint16
	rows       uint16
	nextPaneID uint64
	focus      layout.PaneID
	tree       *layout.Tree
	panes      []snapshotCapturePane
}

type snapshotCapturePane struct {
	id       layout.PaneID
	stableID string
	cwd      string
	// Sealed identities and the copied mutable tail are retained separately, so
	// capture never rotates a live mutable tail merely to persist it.
	sealed vt.HistorySnapshotView
	tail   vt.HistoryView
	// visible is an owned copy taken while pane.mu is held. It must be
	// marshaled only after that lock has been released.
	visible vt.PrimaryVisibleSnapshot
	process *snapcodec.Process
}

// snapshotFailureSignature classifies a persistence failure without retaining
// error text. Error strings often contain object content or filesystem paths,
// neither of which is stable enough for deduplication or suitable for a global
// notice history.
func snapshotFailureSignature(phase string, err error) string {
	if err == nil {
		return phase + ":none"
	}
	switch {
	case errors.Is(err, context.Canceled):
		return phase + ":canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return phase + ":deadline"
	default:
		return fmt.Sprintf("%s:%T", phase, err)
	}
}

const (
	// Routine checkpoints are rate limited from completion; forced teardown
	// checkpoints use the same bounded worker but never consume this window.
	snapshotInterval          = 2 * time.Minute
	snapshotFinalFlushTimeout = time.Second
)
