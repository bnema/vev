package recoveryfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	discardVersion       = 1
	maxDiscardIntents    = 1024
	maxDiscardIntentSize = 64 << 10
)

type Journal struct {
	dir   string
	hooks journalHooks
}

type discardFile struct {
	Version int                  `json:"version"`
	Intent  domain.DiscardIntent `json:"intent"`
}

func New(stateDir string) *Journal { return &Journal{dir: filepath.Join(stateDir, "transactions")} }

func validateDiscard(intent domain.DiscardIntent) error {
	if err := intent.OldRecord.Validate(); err != nil {
		return fmt.Errorf("recovery journal: old record: %w", err)
	}
	if intent.SessionName != intent.OldRecord.Name || intent.OldIncarnation != intent.OldRecord.IncarnationID {
		return errors.New("recovery journal: inconsistent old identity")
	}
	if intent.NewIncarnation == (domain.IncarnationID{}) || intent.NewIncarnation == intent.OldIncarnation || intent.Reason == "" {
		return errors.New("recovery journal: incomplete discard intent")
	}
	return nil
}

func encodeDiscard(intent domain.DiscardIntent) ([]byte, error) {
	if err := validateDiscard(intent); err != nil {
		return nil, err
	}
	return json.Marshal(discardFile{Version: discardVersion, Intent: intent})
}

func decodeDiscard(data []byte) (domain.DiscardIntent, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var file discardFile
	if err := decoder.Decode(&file); err != nil {
		return domain.DiscardIntent{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.DiscardIntent{}, errors.New("recovery journal: trailing data")
	}
	if file.Version != discardVersion {
		return domain.DiscardIntent{}, errors.New("recovery journal: unsupported version")
	}
	return file.Intent, validateDiscard(file.Intent)
}

func (j *Journal) path(id domain.IncarnationID) string {
	return filepath.Join(j.dir, "discard-"+id.String())
}

func (j *Journal) SaveDiscard(ctx context.Context, intent domain.DiscardIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := encodeDiscard(intent)
	if err != nil {
		return err
	}
	path := j.path(intent.OldIncarnation)
	if existing, err := readFileBounded(path, maxDiscardIntentSize); err == nil {
		if !bytes.Equal(existing, data) {
			return errors.New("recovery journal: discard identity collision")
		}
		// Re-sync the parent on idempotent retry: the prior install may have
		// succeeded while its durability acknowledgement failed.
		return j.syncDir(filepath.Dir(path))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return j.atomicWrite(path, data)
}

func (j *Journal) ListDiscards(ctx context.Context) ([]domain.DiscardIntent, error) {
	dir, err := os.Open(j.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries := make([]os.DirEntry, 0, maxDiscardIntents)
	for {
		batch, readErr := dir.ReadDir(maxDiscardIntents + 1)
		for _, entry := range batch {
			if strings.HasPrefix(entry.Name(), ".intent-") && !entry.IsDir() {
				continue
			}
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), "discard-") {
				_ = dir.Close()
				return nil, errors.New("recovery journal: invalid transaction entry")
			}
			entries = append(entries, entry)
			if len(entries) > maxDiscardIntents {
				_ = dir.Close()
				return nil, errors.New("recovery journal: transaction entry limit exceeded")
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = dir.Close()
			return nil, readErr
		}
	}
	if err := dir.Close(); err != nil {
		return nil, err
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].Name() < entries[b].Name() })
	out := make([]domain.DiscardIntent, 0, len(entries))
	seen := make(map[domain.IncarnationID]struct{}, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := readFileBounded(filepath.Join(j.dir, entry.Name()), maxDiscardIntentSize)
		if err != nil {
			return nil, err
		}
		intent, err := decodeDiscard(data)
		if err != nil {
			return nil, err
		}
		if entry.Name() != filepath.Base(j.path(intent.OldIncarnation)) {
			return nil, errors.New("recovery journal: non-canonical intent filename")
		}
		if _, ok := seen[intent.OldIncarnation]; ok {
			return nil, errors.New("recovery journal: duplicate discard identity")
		}
		seen[intent.OldIncarnation] = struct{}{}
		out = append(out, intent)
	}
	return out, nil
}

func (j *Journal) DeleteDiscard(ctx context.Context, id domain.IncarnationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == (domain.IncarnationID{}) {
		return errors.New("recovery journal: zero discard identity")
	}
	if err := safedir.EnsurePrivate(j.dir); err != nil {
		return err
	}
	if err := os.Remove(j.path(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return j.syncDir(j.dir)
}

func readFileBounded(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) > limit {
		return nil, errors.New("recovery journal: intent size limit exceeded")
	}
	return data, nil
}
