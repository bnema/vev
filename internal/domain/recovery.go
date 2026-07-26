package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// IncarnationID permanently distinguishes uses of the same session name.
type IncarnationID [16]byte

// RecoveryState describes whether and how a named session can be restored.
type RecoveryState uint8

const (
	RecoveryFresh RecoveryState = iota + 1
	RecoveryHealthy
	RecoveryDegraded
	RecoveryDeleting
)

// CheckpointRef identifies one immutable snapshot manifest.
type CheckpointRef struct {
	Generation     uint64
	ManifestDigest [32]byte
}

// CatalogueRecord is the durable source of truth for one named session.
type CatalogueRecord struct {
	Name           string
	IncarnationID  IncarnationID
	Cwd            string
	CreatedAt      int64
	UpdatedAt      int64
	LastUsedSeq    uint64
	TabNames       []string
	RecoveryState  RecoveryState
	Committed      *CheckpointRef
	Fallbacks      [2]*CheckpointRef
	DegradedReason string
}

// CatalogueMetadataUpdate changes mutable runtime metadata for one catalogue
// incarnation without carrying authority-owned recovery or checkpoint fields.
type CatalogueMetadataUpdate struct {
	Name          string
	IncarnationID IncarnationID
	Cwd           *string
	UpdatedAt     *int64
	LastUsedSeq   *uint64
	TabNames      *[]string
}

// MetadataUpdate returns a complete mutable-metadata update for r. Catalogue
// implementations retain all recovery and checkpoint fields they own.
func (r CatalogueRecord) MetadataUpdate() CatalogueMetadataUpdate {
	cwd, updatedAt, lastUsedSeq := r.Cwd, r.UpdatedAt, r.LastUsedSeq
	tabNames := append([]string(nil), r.TabNames...)
	return CatalogueMetadataUpdate{
		Name: r.Name, IncarnationID: r.IncarnationID,
		Cwd: &cwd, UpdatedAt: &updatedAt, LastUsedSeq: &lastUsedSeq, TabNames: &tabNames,
	}
}

// DiscardIntent records a requested replacement of uncertain persisted state.
type DiscardIntent struct {
	OldRecord      CatalogueRecord
	OldIncarnation IncarnationID
	NewIncarnation IncarnationID
	SessionName    string
	Reason         string
}

// QuarantineDescriptor records persisted state retained during replacement.
type QuarantineDescriptor struct {
	OldRecord              CatalogueRecord
	OldIncarnation         IncarnationID
	ReplacementIncarnation IncarnationID
	SessionName            string
	Reason                 string
}

// DeletionTombstone identifies exactly one deleted session incarnation.
type DeletionTombstone struct {
	Name          string
	IncarnationID IncarnationID
}

// Validate rejects incomplete deletion identities.
func (t DeletionTombstone) Validate() error {
	if err := ValidateSessionName(t.Name); err != nil {
		return err
	}
	if t.IncarnationID == (IncarnationID{}) {
		return errors.New("zero deletion incarnation ID")
	}
	return nil
}

// NewIncarnationID reads a new non-zero identity from r.
func NewIncarnationID(r io.Reader) (IncarnationID, error) {
	var id IncarnationID
	if _, err := io.ReadFull(r, id[:]); err != nil {
		return id, err
	}
	if id == (IncarnationID{}) {
		return id, errors.New("zero incarnation ID")
	}
	return id, nil
}

func (id IncarnationID) String() string {
	return hex.EncodeToString(id[:])
}

// MarshalText emits the canonical lowercase hexadecimal identity.
func (id IncarnationID) MarshalText() ([]byte, error) {
	if id == (IncarnationID{}) {
		return nil, errors.New("zero incarnation ID")
	}
	return []byte(id.String()), nil
}

// UnmarshalText accepts only a canonical lowercase non-zero identity.
func (id *IncarnationID) UnmarshalText(text []byte) error {
	if len(text) != 32 || strings.ToLower(string(text)) != string(text) {
		return errors.New("invalid incarnation ID")
	}
	var decoded IncarnationID
	if _, err := hex.Decode(decoded[:], text); err != nil {
		return fmt.Errorf("invalid incarnation ID: %w", err)
	}
	if decoded == (IncarnationID{}) {
		return errors.New("zero incarnation ID")
	}
	*id = decoded
	return nil
}

// Validate rejects catalogue states that cannot be restored deterministically.
func (r CatalogueRecord) Validate() error {
	if err := ValidateSessionName(r.Name); err != nil {
		return err
	}
	if r.IncarnationID == (IncarnationID{}) {
		return errors.New("zero catalogue incarnation ID")
	}

	switch r.RecoveryState {
	case RecoveryFresh:
		if r.Committed != nil || r.Fallbacks[0] != nil || r.Fallbacks[1] != nil {
			return errors.New("fresh session has checkpoint references")
		}
	case RecoveryHealthy:
		if r.Committed == nil {
			return errors.New("healthy session has no committed checkpoint")
		}
	case RecoveryDegraded:
		if r.Committed == nil {
			return errors.New("degraded session has no committed checkpoint")
		}
		if r.DegradedReason == "" {
			return errors.New("degraded session has no reason")
		}
	case RecoveryDeleting:
		// A fresh or checkpointed session may be deleted.
	default:
		return errors.New("invalid recovery state")
	}

	if r.RecoveryState != RecoveryDegraded && r.DegradedReason != "" {
		return errors.New("non-degraded session has degraded reason")
	}
	if r.Fallbacks[1] != nil && r.Fallbacks[0] == nil {
		return errors.New("second fallback populated without first fallback")
	}
	if r.Committed == nil && (r.Fallbacks[0] != nil || r.Fallbacks[1] != nil) {
		return errors.New("fallback populated without committed checkpoint")
	}

	seen := make(map[uint64]struct{}, 3)
	var previous uint64
	for i, ref := range []*CheckpointRef{r.Committed, r.Fallbacks[0], r.Fallbacks[1]} {
		if ref == nil {
			continue
		}
		if ref.Generation == 0 {
			return errors.New("zero checkpoint generation")
		}
		if ref.ManifestDigest == ([32]byte{}) {
			return errors.New("zero checkpoint manifest digest")
		}
		if _, ok := seen[ref.Generation]; ok {
			return errors.New("duplicate checkpoint generation")
		}
		seen[ref.Generation] = struct{}{}
		if i > 0 && previous <= ref.Generation {
			return errors.New("checkpoint generations are not newest to oldest")
		}
		previous = ref.Generation
	}
	return nil
}
