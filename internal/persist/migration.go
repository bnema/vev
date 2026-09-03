package persist

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/bnema/vev/internal/domain"
)

const legacyCatalogueBackupSuffix = ".pre-v6.bak"

// Migration describes an automatic, lossless catalogue format migration.
type Migration struct {
	Performed     bool
	SourceFormats []uint16
	RecordCount   int
	BackupPath    string
}

func (p *Persister) migrateLegacyRecords(path string, records []domain.CatalogueRecord) (Migration, error) {
	if p == nil || p.store == nil {
		return Migration{}, errPersistenceUnavailable
	}
	formats, err := p.recordFormats()
	if err != nil {
		return Migration{}, err
	}
	legacy := false
	for _, format := range formats {
		if format != catalogueRecordVersion {
			legacy = true
			break
		}
	}
	if !legacy {
		return Migration{}, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return Migration{}, fmt.Errorf("read catalogue backup source: %w", err)
	}
	backupPath := path + legacyCatalogueBackupSuffix
	if err := createMatchingBackup(backupPath, raw); err != nil {
		return Migration{}, err
	}

	updates := make(map[string]*domain.CatalogueRecord, len(records))
	for i := range records {
		record := records[i]
		updates[record.Name] = &record
	}
	if err := p.Apply(updates); err != nil {
		return Migration{}, fmt.Errorf("write migrated catalogue: %w", err)
	}
	return Migration{
		Performed:     true,
		SourceFormats: formats,
		RecordCount:   len(records),
		BackupPath:    backupPath,
	}, nil
}

func (p *Persister) recordFormats() ([]uint16, error) {
	formats := make(map[uint16]struct{})
	var decodeErr error
	p.store.Range(func(key, value []byte) bool {
		stored, err := decodeStoredRecordValue(string(key), value)
		if err != nil {
			decodeErr = err
			return false
		}
		formats[stored.formatVersion] = struct{}{}
		return true
	})
	if decodeErr != nil {
		return nil, decodeErr
	}
	out := make([]uint16, 0, len(formats))
	for format := range formats {
		out = append(out, format)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func createMatchingBackup(path string, data []byte) error {
	matched, err := existingBackupMatches(path, data)
	if err != nil || matched {
		return err
	}

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create catalogue backup temporary file: %w", err)
	}
	cleanup := func(err error) error {
		return errors.Join(err, file.Close(), os.Remove(tmp))
	}
	if err := file.Chmod(0o600); err != nil {
		return cleanup(fmt.Errorf("set catalogue backup permissions: %w", err))
	}
	if err := writeBackup(file, data); err != nil {
		return cleanup(fmt.Errorf("write catalogue backup: %w", err))
	}
	if err := file.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync catalogue backup: %w", err))
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close catalogue backup: %w", err)
	}
	if err := os.Link(tmp, path); err != nil {
		_ = os.Remove(tmp)
		if errors.Is(err, os.ErrExist) {
			matched, matchErr := existingBackupMatches(path, data)
			if matchErr != nil {
				return matchErr
			}
			if !matched {
				return fmt.Errorf("persist: catalogue backup %q disappeared during migration", path)
			}
		} else {
			return fmt.Errorf("publish catalogue backup: %w", err)
		}
	} else if err := os.Remove(tmp); err != nil {
		return fmt.Errorf("remove catalogue backup temporary file: %w", err)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open catalogue backup directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync catalogue backup directory: %w", err)
	}
	return nil
}

func existingBackupMatches(path string, data []byte) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat existing catalogue backup: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return false, fmt.Errorf("persist: catalogue backup %q is not a private regular file", path)
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read existing catalogue backup: %w", err)
	}
	if !bytes.Equal(existing, data) {
		return false, fmt.Errorf("persist: catalogue backup %q does not match migration source", path)
	}
	return true, nil
}

func writeBackup(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
