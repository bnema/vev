package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	repositorySessionsDir = "sessions"
	repositoryObjectsDir  = "objects"
	repositoryGenerations = "generations"
	repositoryHead        = "HEAD"
	generationWidth       = 20
	maxRepositoryRead     = maxSnapshotFileSize
)

// Repository is the crash-safe, content-addressed session snapshot store.
// Store remains available separately for the one-way legacy bridge.
type Repository struct {
	dir    string
	legacy *Store
	locks  sync.Map // map[string]*sync.Mutex
	hooks  repositoryHooks
}

// repositoryHooks makes each persistence boundary fault-injectable. Hooks run
// immediately before their respective syscall and are intentionally package
// private so production callers cannot weaken repository guarantees.
type repositoryHooks struct {
	beforeBlobWrite     func(string) error
	beforeManifestWrite func(string) error
	beforeHeadWrite     func(string) error
	createTemp          func(string) error
	writeTemp           func(string) error
	syncFile            func(string) error
	closeFile           func(string) error
	installImmutable    func(string) error
	rename              func(string) error
	syncDirectory       func(string) error
	remove              func(string) error
}

var _ ports.SnapshotRepository = (*Repository)(nil)
var _ ports.LegacySnapshotSource = (*Repository)(nil)

// NewRepository creates a repository rooted at dir. It does not create files
// until the first publication, so merely constructing it is side-effect free.
func NewRepository(dir string) *Repository { return &Repository{dir: dir, legacy: NewStore(dir)} }

func (r *Repository) Publish(ctx context.Context, publication ports.SnapshotPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(publication.Name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()

	manifest, refs, supplied, err := validatePublication(publication)
	if err != nil {
		return err
	}
	_ = manifest
	if err := r.ensureSession(key); err != nil {
		return err
	}

	current, currentManifest, err := r.currentGeneration(ctx, publication.Name, key)
	if err != nil {
		return err
	}
	if publication.Generation < current || publication.Generation > current+1 {
		return fmt.Errorf("snapshot generation %d, current %d: immutable conflict", publication.Generation, current)
	}
	if publication.Generation == current {
		if !equalBytes(currentManifest, publication.Manifest) {
			return fmt.Errorf("snapshot generation %d: immutable conflict", publication.Generation)
		}
		return nil
	}

	for digest, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := r.objectPath(key, digest)
		exists, err := verifyObjectFile(path, digest, ref)
		if err != nil {
			return fmt.Errorf("verify object %x: %w", digest, err)
		}
		if exists {
			continue
		}
		object, ok := supplied[digest]
		if !ok {
			return fmt.Errorf("missing referenced object %x", digest)
		}
		if r.hooks.beforeBlobWrite != nil {
			if err := r.hooks.beforeBlobWrite(path); err != nil {
				return err
			}
		}
		if err := r.writeImmutable(path, object.Data, func(existing []byte) error {
			if sha256.Sum256(existing) != digest || !validObject(existing, ref) {
				return fmt.Errorf("existing immutable object is invalid")
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write object: %w", err)
		}
	}

	manifestPath := r.manifestPath(key, publication.Generation)
	existing, exists, err := readOptionalBounded(manifestPath)
	if err != nil {
		return err
	}
	if exists {
		if !equalBytes(existing, publication.Manifest) {
			return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
		}
	} else {
		if r.hooks.beforeManifestWrite != nil {
			if err := r.hooks.beforeManifestWrite(manifestPath); err != nil {
				return err
			}
		}
		if err := r.writeImmutable(manifestPath, publication.Manifest, func(existing []byte) error {
			if !equalBytes(existing, publication.Manifest) {
				return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if r.hooks.beforeHeadWrite != nil {
		if err := r.hooks.beforeHeadWrite(r.headPath(key)); err != nil {
			return err
		}
	}
	if err := r.writeMutable(r.headPath(key), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest))); err != nil {
		return fmt.Errorf("write HEAD: %w", err)
	}
	return nil
}

func (r *Repository) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(r.dir, repositorySessionsDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot sessions: %w", err)
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() || !canonicalSessionKey(entry.Name()) {
			continue
		}
		numbers, err := r.generationNumbers(entry.Name())
		if err != nil {
			continue
		}
		for _, number := range numbers {
			data, err := readBounded(r.manifestPath(entry.Name(), number))
			if err != nil {
				continue
			}
			manifest, err := codec.UnmarshalManifest(data)
			if err != nil || sessionKey(manifest.Name) != entry.Name() {
				continue
			}
			generation, err := r.loadGeneration(ctx, manifest.Name, entry.Name(), number)
			if err == nil {
				out = append(out, generation.Name)
				break
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func (r *Repository) Load(ctx context.Context, name string) (ports.SnapshotGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	key := sessionKey(name)
	preferred, preferredDigest, err := r.readHead(key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		preferred = 0
	}
	candidates, err := r.generationNumbers(key)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	if preferred != 0 {
		candidates = append([]uint64{preferred}, removeGeneration(candidates, preferred)...)
	}
	skipped := false
	for _, generation := range candidates {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err != nil || (generation == preferred && sha256.Sum256(got.Manifest) != preferredDigest) {
			skipped = true
			continue
		}
		got.Fallback = skipped || (preferred != 0 && generation != preferred)
		return got, nil
	}
	return ports.SnapshotGeneration{}, fmt.Errorf("no complete snapshot generation for %q", name)
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()
	path := r.sessionPath(key)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat snapshot session: %w", err)
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("delete snapshot session: %w", err)
	}
	return r.syncDirectory(filepath.Join(r.dir, repositorySessionsDir))
}

// Maintain removes abandoned temporary and quarantine files. Immutable data is
// deliberately never garbage-collected here because older generations need it.
func (r *Repository) Maintain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return filepath.WalkDir(r.dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		base := entry.Name()
		if strings.HasPrefix(base, ".tmp-") || strings.HasPrefix(base, ".quarantine-") {
			if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			return r.syncDirectory(filepath.Dir(path))
		}
		return nil
	})
}

func (r *Repository) LoadLegacy(ctx context.Context) ([]ports.LegacySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	blobs, err := r.legacy.Load()
	if err != nil {
		return nil, err
	}
	out := make([]ports.LegacySnapshot, len(blobs))
	for i, blob := range blobs {
		out[i] = ports.LegacySnapshot{Name: blob.Name, Data: append([]byte(nil), blob.Data...)}
	}
	return out, nil
}

func (r *Repository) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.legacy.Delete(name)
}

func validatePublication(p ports.SnapshotPublication) (codec.Manifest, map[ports.SnapshotDigest]codec.ObjectRef, map[ports.SnapshotDigest]ports.SnapshotObject, error) {
	manifest, err := codec.UnmarshalManifest(p.Manifest)
	if err != nil {
		return codec.Manifest{}, nil, nil, fmt.Errorf("invalid manifest: %w", err)
	}
	if manifest.Name != p.Name || manifest.Generation != p.Generation || p.Generation == 0 {
		return codec.Manifest{}, nil, nil, fmt.Errorf("manifest name or generation does not match publication")
	}
	refs := manifestRefs(manifest)
	if refs == nil {
		return codec.Manifest{}, nil, nil, fmt.Errorf("conflicting manifest references")
	}
	if !withinGenerationBudget(len(p.Manifest), refs) {
		return codec.Manifest{}, nil, nil, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest]ports.SnapshotObject, len(p.Objects))
	for _, object := range p.Objects {
		if sha256.Sum256(object.Data) != object.Digest {
			return codec.Manifest{}, nil, nil, fmt.Errorf("object digest mismatch")
		}
		ref, used := refs[object.Digest]
		if !used {
			return codec.Manifest{}, nil, nil, fmt.Errorf("unreferenced object")
		}
		kind, payload, err := codec.PreflightObject(object.Data)
		if err != nil || kind != ref.Kind || len(object.Data) != int(ref.Size) || len(payload) == 0 {
			return codec.Manifest{}, nil, nil, fmt.Errorf("invalid object envelope")
		}
		if old, duplicate := objects[object.Digest]; duplicate && !equalBytes(old.Data, object.Data) {
			return codec.Manifest{}, nil, nil, fmt.Errorf("conflicting object")
		}
		objects[object.Digest] = ports.SnapshotObject{Digest: object.Digest, Data: append([]byte(nil), object.Data...)}
	}
	return manifest, refs, objects, nil
}

func withinGenerationBudget(manifestSize int, refs map[ports.SnapshotDigest]codec.ObjectRef) bool {
	if manifestSize < 0 || manifestSize > maxRepositoryRead {
		return false
	}
	total := uint64(manifestSize)
	for _, ref := range refs {
		if uint64(ref.Size) > uint64(maxRepositoryRead)-total {
			return false
		}
		total += uint64(ref.Size)
	}
	return true
}

func manifestRefs(manifest codec.Manifest) map[ports.SnapshotDigest]codec.ObjectRef {
	refs := make(map[ports.SnapshotDigest]codec.ObjectRef)
	add := func(ref codec.ObjectRef) bool {
		if old, ok := refs[ref.Digest]; ok && old != ref {
			return false
		}
		refs[ref.Digest] = ref
		return true
	}
	for _, tab := range manifest.Tabs {
		for _, pane := range tab.Panes {
			for _, ref := range pane.Sealed {
				if !add(ref) {
					return nil
				}
			}
			if !add(pane.Tail) || !add(pane.Visible) {
				return nil
			}
		}
	}
	return refs
}

func (r *Repository) currentGeneration(ctx context.Context, name, key string) (uint64, []byte, error) {
	generation, digest, err := r.readHead(key)
	if err == nil {
		got, err := r.loadGeneration(ctx, name, key, generation)
		if err == nil && sha256.Sum256(got.Manifest) == digest {
			return generation, got.Manifest, nil
		}
	}
	candidates, err := r.generationNumbers(key)
	if err != nil {
		return 0, nil, err
	}
	for _, candidate := range candidates {
		if got, err := r.loadGeneration(ctx, name, key, candidate); err == nil {
			return candidate, got.Manifest, nil
		}
	}
	return 0, nil, nil
}

func (r *Repository) loadGeneration(ctx context.Context, name, key string, generation uint64) (ports.SnapshotGeneration, error) {
	data, err := readBounded(r.manifestPath(key, generation))
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.Name != name || manifest.Generation != generation {
		return ports.SnapshotGeneration{}, fmt.Errorf("invalid manifest")
	}
	refs := manifestRefs(manifest)
	if refs == nil || !withinGenerationBudget(len(data), refs) {
		return ports.SnapshotGeneration{}, fmt.Errorf("snapshot generation too large")
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(refs))
	for digest, ref := range refs {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		object, err := readBounded(r.objectPath(key, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, ref) {
			return ports.SnapshotGeneration{}, fmt.Errorf("invalid object")
		}
		objects[digest] = append([]byte(nil), object...)
	}
	return ports.SnapshotGeneration{Name: manifest.Name, Generation: generation, Manifest: append([]byte(nil), data...), Objects: objects}, nil
}

func validObject(data []byte, ref codec.ObjectRef) bool {
	kind, payload, err := codec.PreflightObject(data)
	return err == nil && kind == ref.Kind && len(payload) > 0 && len(data) == int(ref.Size)
}

func verifyObjectFile(path string, digest ports.SnapshotDigest, ref codec.ObjectRef) (bool, error) {
	data, exists, err := readOptionalBounded(path)
	if err != nil || !exists {
		return exists, err
	}
	if sha256.Sum256(data) != digest || !validObject(data, ref) {
		return true, fmt.Errorf("existing immutable object is invalid")
	}
	return true, nil
}

func (r *Repository) ensureSession(key string) error {
	for _, directory := range []struct {
		path  string
		phase string
	}{
		{r.dir, "repository"},
		{filepath.Join(r.dir, repositorySessionsDir), "sessions"},
		{r.sessionPath(key), "session"},
		{filepath.Join(r.sessionPath(key), repositoryObjectsDir), "objects"},
		{filepath.Join(r.sessionPath(key), repositoryGenerations), "generations"},
	} {
		if err := r.ensurePrivateDirectoryPhase(directory.path, directory.phase); err != nil {
			return fmt.Errorf("create snapshot repository directory: %w", err)
		}
	}
	return nil
}

func (r *Repository) ensurePrivateDirectory(dir string) error {
	return r.ensurePrivateDirectoryPhase(dir, "snapshot directory")
}

func (r *Repository) ensurePrivateDirectoryPhase(dir, phase string) error {
	_, err := os.Lstat(dir)
	created := errors.Is(err, os.ErrNotExist)
	if err != nil && !created {
		return err
	}
	if created {
		parent := filepath.Dir(dir)
		if _, err := os.Lstat(parent); errors.Is(err, os.ErrNotExist) {
			if err := r.ensurePrivateDirectoryPhase(parent, "snapshot directory"); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
	}
	if created {
		if err := r.syncDirectory(filepath.Dir(dir)); err != nil {
			return fmt.Errorf("%s parent directory sync: %w", phase, err)
		}
	}
	return nil
}

func (r *Repository) sessionPath(key string) string {
	return filepath.Join(r.dir, repositorySessionsDir, key)
}
func (r *Repository) objectPath(key string, digest ports.SnapshotDigest) string {
	hexDigest := hex.EncodeToString(digest[:])
	return filepath.Join(r.sessionPath(key), repositoryObjectsDir, hexDigest[:2], hexDigest)
}
func (r *Repository) manifestPath(key string, generation uint64) string {
	return filepath.Join(r.sessionPath(key), repositoryGenerations, generationFilename(generation))
}
func (r *Repository) headPath(key string) string {
	return filepath.Join(r.sessionPath(key), repositoryHead)
}
func (r *Repository) sessionLock(key string) *sync.Mutex {
	lock, _ := r.locks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func sessionKey(name string) string {
	if safeNameRE.MatchString(name) && name != "." && name != ".." {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return "@" + hex.EncodeToString(sum[:])
}
func canonicalSessionKey(key string) bool {
	if safeNameRE.MatchString(key) && key != "." && key != ".." {
		return true
	}
	if len(key) != 65 || key[0] != '@' {
		return false
	}
	_, err := hex.DecodeString(key[1:])
	return err == nil && strings.ToLower(key[1:]) == key[1:]
}
func generationFilename(generation uint64) string {
	return fmt.Sprintf("%0*d.manifest", generationWidth, generation)
}
func parseGenerationFilename(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".manifest") || len(name) != generationWidth+len(".manifest") {
		return 0, false
	}
	n := strings.TrimSuffix(name, ".manifest")
	generation, err := strconv.ParseUint(n, 10, 64)
	return generation, err == nil && generationFilename(generation) == name && generation != 0
}

func (r *Repository) generationNumbers(key string) ([]uint64, error) {
	entries, err := os.ReadDir(filepath.Join(r.sessionPath(key), repositoryGenerations))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	numbers := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if generation, ok := parseGenerationFilename(entry.Name()); ok {
			numbers = append(numbers, generation)
		}
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] > numbers[j] })
	return numbers, nil
}
func removeGeneration(numbers []uint64, generation uint64) []uint64 {
	out := numbers[:0]
	for _, n := range numbers {
		if n != generation {
			out = append(out, n)
		}
	}
	return out
}

func marshalHead(generation uint64, digest ports.SnapshotDigest) []byte {
	out := make([]byte, 4+8+sha256.Size)
	copy(out, "VEVH")
	binary.BigEndian.PutUint64(out[4:12], generation)
	copy(out[12:], digest[:])
	return out
}
func (r *Repository) readHead(key string) (uint64, ports.SnapshotDigest, error) {
	data, err := readBounded(r.headPath(key))
	if err != nil {
		return 0, ports.SnapshotDigest{}, err
	}
	if len(data) != 4+8+sha256.Size || string(data[:4]) != "VEVH" {
		return 0, ports.SnapshotDigest{}, fmt.Errorf("invalid HEAD")
	}
	generation := binary.BigEndian.Uint64(data[4:12])
	var digest ports.SnapshotDigest
	copy(digest[:], data[12:])
	if generation == 0 {
		return 0, ports.SnapshotDigest{}, fmt.Errorf("invalid HEAD")
	}
	return generation, digest, nil
}

// writeImmutable publishes data with link(2), whose EEXIST behavior prevents a
// raced target from being overwritten. A competing target is accepted only
// after verifier reads it through the same secure descriptor path as Load.
func (r *Repository) writeImmutable(path string, data []byte, verifier func([]byte) error) error {
	dir := filepath.Dir(path)
	phase := "object shard"
	if filepath.Base(dir) == repositoryGenerations {
		phase = "generation"
	}
	if err := r.ensurePrivateDirectoryPhase(dir, phase); err != nil {
		return err
	}
	temp, err := r.createTemp(dir)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	cleanup := func(cause error) error {
		if !closed {
			if err := r.closeFile(temp); err != nil {
				cause = errors.Join(cause, err)
			}
			closed = true
		}
		if err := r.remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			cause = errors.Join(cause, err)
		} else if err == nil {
			cause = errors.Join(cause, r.syncDirectory(dir))
		}
		return cause
	}
	if err := temp.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if err := r.writeFile(temp, data); err != nil {
		return cleanup(err)
	}
	if err := r.syncFile(temp); err != nil {
		return cleanup(err)
	}
	if err := r.closeFile(temp); err != nil {
		closed = true
		return cleanup(err)
	}
	closed = true
	if err := r.installImmutable(tempPath, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := readBounded(path)
			if readErr != nil {
				return cleanup(readErr)
			}
			if verifyErr := verifier(existing); verifyErr != nil {
				return cleanup(verifyErr)
			}
			return cleanup(nil)
		}
		return cleanup(err)
	}
	if err := r.syncDirectory(dir); err != nil {
		return cleanup(err)
	}
	return cleanup(nil)
}

// writeMutable is used only for HEAD, the sole authoritative pointer allowed
// to advance. The old or fully synced new HEAD is therefore always recoverable.
func (r *Repository) writeMutable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := r.ensurePrivateDirectory(dir); err != nil {
		return err
	}
	temp, err := r.createTemp(dir)
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	closed := false
	cleanup := func(cause error) error {
		if !closed {
			if err := r.closeFile(temp); err != nil {
				cause = errors.Join(cause, err)
			}
			closed = true
		}
		if err := r.remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			cause = errors.Join(cause, err)
		} else if err == nil {
			cause = errors.Join(cause, r.syncDirectory(dir))
		}
		return cause
	}
	if err := temp.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	if err := r.writeFile(temp, data); err != nil {
		return cleanup(err)
	}
	if err := r.syncFile(temp); err != nil {
		return cleanup(err)
	}
	if err := r.closeFile(temp); err != nil {
		closed = true
		return cleanup(err)
	}
	closed = true
	if err := r.rename(tempPath, path); err != nil {
		return cleanup(err)
	}
	if err := r.syncDirectory(dir); err != nil {
		return cleanup(err)
	}
	// rename consumed tempPath. Its directory sync above persists both rename
	// and the absence of the temporary entry.
	return nil
}

func (r *Repository) createTemp(dir string) (*os.File, error) {
	if r.hooks.createTemp != nil {
		if err := r.hooks.createTemp(dir); err != nil {
			return nil, err
		}
	}
	return os.CreateTemp(dir, ".tmp-")
}
func (r *Repository) writeFile(f *os.File, data []byte) error {
	if r.hooks.writeTemp != nil {
		if err := r.hooks.writeTemp(f.Name()); err != nil {
			return err
		}
	}
	_, err := f.Write(data)
	return err
}
func (r *Repository) syncFile(f *os.File) error {
	if r.hooks.syncFile != nil {
		if err := r.hooks.syncFile(f.Name()); err != nil {
			return err
		}
	}
	return f.Sync()
}
func (r *Repository) closeFile(f *os.File) error {
	var injected error
	if r.hooks.closeFile != nil {
		injected = r.hooks.closeFile(f.Name())
	}
	closeErr := f.Close()
	if injected != nil {
		return injected
	}
	return closeErr
}
func (r *Repository) installImmutable(oldPath, newPath string) error {
	if r.hooks.installImmutable != nil {
		if err := r.hooks.installImmutable(newPath); err != nil {
			return err
		}
	}
	return os.Link(oldPath, newPath)
}
func (r *Repository) rename(oldPath, newPath string) error {
	if r.hooks.rename != nil {
		if err := r.hooks.rename(newPath); err != nil {
			return err
		}
	}
	return os.Rename(oldPath, newPath)
}
func (r *Repository) remove(path string) error {
	if r.hooks.remove != nil {
		if err := r.hooks.remove(path); err != nil {
			return err
		}
	}
	return os.Remove(path)
}
func (r *Repository) syncDirectory(dir string) error {
	if r.hooks.syncDirectory != nil {
		if err := r.hooks.syncDirectory(dir); err != nil {
			return err
		}
	}
	return syncDirectory(dir)
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func readOptionalBounded(path string) ([]byte, bool, error) {
	data, err := readBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}

// readBounded opens the final component with O_NOFOLLOW, validates the opened
// descriptor, then reads a hard bounded amount. It deliberately never trusts
// path metadata obtained before opening the descriptor.
func readBounded(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open snapshot file")
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		_ = f.Close()
		return nil, err
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = f.Close()
		return nil, fmt.Errorf("not a regular file")
	}
	if int(stat.Uid) != os.Geteuid() {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file is not owned by effective uid")
	}
	if stat.Mode&0o077 != 0 {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file has unsafe permissions")
	}
	if stat.Size < 0 || stat.Size > int64(maxRepositoryRead) {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot file too large")
	}
	data, readErr := io.ReadAll(io.LimitReader(f, int64(maxRepositoryRead)+1))
	closeErr := f.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > maxRepositoryRead {
		return nil, fmt.Errorf("snapshot file too large")
	}
	return data, nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
