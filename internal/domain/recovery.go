package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

// IncarnationID permanently distinguishes uses of the same session name.
type IncarnationID [16]byte

// CheckpointRef identifies one immutable snapshot manifest.
type CheckpointRef struct {
	Generation     uint64
	ManifestDigest [32]byte
}

// CatalogueTabRecord is the durable ordered metadata for one stopped tab.
// StableID is empty for records migrated from the legacy TabNames-only format.
type CatalogueTabRecord struct {
	StableID TabStableID
	Name     string
}

// CatalogueRecord is the durable source of truth for one named session.
type CatalogueRecord struct {
	Name          string
	IncarnationID IncarnationID
	Cwd           string
	CreatedAt     int64
	UpdatedAt     int64
	LastUsedSeq   uint64
	// TabNames remains the compatibility metadata consumed by older recovery
	// paths. TabRecords is authoritative when present and retains trailing
	// unnamed tabs instead of silently trimming them.
	TabNames       []string
	TabRecords     []CatalogueTabRecord
	Committed      *CheckpointRef
	DegradedReason string
}

// Equal reports canonical value equality between checkpoint references.
func (r *CheckpointRef) Equal(other *CheckpointRef) bool {
	if r == nil || other == nil {
		return r == nil && other == nil
	}
	return *r == *other
}

// Equal reports canonical value equality, including ordered tab names and
// pointed-to checkpoint values rather than pointer identity.
func (r CatalogueRecord) Equal(other CatalogueRecord) bool {
	return r.Name == other.Name && r.IncarnationID == other.IncarnationID && r.Cwd == other.Cwd &&
		r.CreatedAt == other.CreatedAt && r.UpdatedAt == other.UpdatedAt && r.LastUsedSeq == other.LastUsedSeq &&
		slices.Equal(r.TabNames, other.TabNames) && slices.Equal(r.TabRecords, other.TabRecords) && r.Committed.Equal(other.Committed) &&
		r.DegradedReason == other.DegradedReason
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
	TabRecords    *[]CatalogueTabRecord
}

// MetadataUpdate returns a complete mutable-metadata update for r. Catalogue
// implementations retain all recovery and checkpoint fields they own.
func (r CatalogueRecord) MetadataUpdate() CatalogueMetadataUpdate {
	cwd, updatedAt, lastUsedSeq := r.Cwd, r.UpdatedAt, r.LastUsedSeq
	tabNames := append([]string(nil), r.TabNames...)
	tabRecords := append([]CatalogueTabRecord(nil), r.TabRecords...)
	return CatalogueMetadataUpdate{
		Name: r.Name, IncarnationID: r.IncarnationID,
		Cwd: &cwd, UpdatedAt: &updatedAt, LastUsedSeq: &lastUsedSeq, TabNames: &tabNames, TabRecords: &tabRecords,
	}
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

	for _, tab := range r.TabRecords {
		if tab.StableID != "" {
			if err := ValidateTabStableID(tab.StableID); err != nil {
				return errors.New("invalid catalogue tab metadata")
			}
		}
		if !utf8.ValidString(tab.Name) {
			return errors.New("invalid catalogue tab metadata")
		}
	}
	if r.DegradedReason != "" && r.Committed == nil {
		return errors.New("broken session has no committed checkpoint")
	}
	if r.Committed != nil {
		if r.Committed.Generation == 0 {
			return errors.New("zero checkpoint generation")
		}
		if r.Committed.ManifestDigest == ([32]byte{}) {
			return errors.New("zero checkpoint manifest digest")
		}
	}
	return nil
}
