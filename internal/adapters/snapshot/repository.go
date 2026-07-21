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

type repositoryHooks struct {
	beforeBlobWrite     func(string) error
	beforeManifestWrite func(string) error
	beforeHeadWrite     func(string) error
	writeFile           func(string, []byte) error
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
		return nil // exact replay; validation above also rejects altered objects.
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
		object, supplied := supplied[digest]
		if !supplied {
			return fmt.Errorf("missing referenced object %x", digest)
		}
		if r.hooks.beforeBlobWrite != nil {
			if err := r.hooks.beforeBlobWrite(path); err != nil {
				return err
			}
		}
		if err := r.atomicWrite(path, object.Data); err != nil {
			return fmt.Errorf("write object: %w", err)
		}
	}

	manifestPath := r.manifestPath(key, publication.Generation)
	if existing, exists, err := readOptionalBounded(manifestPath); err != nil {
		return err
	} else if exists {
		if !equalBytes(existing, publication.Manifest) {
			return fmt.Errorf("manifest generation %d: immutable conflict", publication.Generation)
		}
	} else {
		if r.hooks.beforeManifestWrite != nil {
			if err := r.hooks.beforeManifestWrite(manifestPath); err != nil {
				return err
			}
		}
		if err := r.atomicWrite(manifestPath, publication.Manifest); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	if r.hooks.beforeHeadWrite != nil {
		if err := r.hooks.beforeHeadWrite(r.headPath(key)); err != nil {
			return err
		}
	}
	if err := r.atomicWrite(r.headPath(key), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest))); err != nil {
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
		// Loading is intentionally authoritative: a directory is listed only
		// when its complete newest valid generation can be restored. Unsafe
		// keys are not reversible, so obtain their name from a validated VEVM.
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
	// HEAD is only a hint. A torn/corrupt newest generation must never hide an
	// older complete generation.
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
	err := os.RemoveAll(r.sessionPath(key))
	if err != nil {
		return fmt.Errorf("delete snapshot session: %w", err)
	}
	if _, err := os.Stat(r.dir); err == nil {
		return syncDirectory(r.dir)
	}
	return nil
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
			return os.Remove(path)
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
	if refs == nil {
		return ports.SnapshotGeneration{}, fmt.Errorf("conflicting manifest references")
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(refs))
	total := len(data)
	for digest, ref := range refs {
		if err := ctx.Err(); err != nil {
			return ports.SnapshotGeneration{}, err
		}
		// Ref sizes were validated by the VEVM preflight. Check the aggregate
		// before allocating another object so hostile repositories cannot exceed
		// the restore budget transiently.
		if uint64(ref.Size) > uint64(maxRepositoryRead-total) {
			return ports.SnapshotGeneration{}, fmt.Errorf("snapshot generation too large")
		}
		object, err := readBounded(r.objectPath(key, digest))
		if err != nil {
			return ports.SnapshotGeneration{}, err
		}
		total += len(object)
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
	return true, func() error {
		if sha256.Sum256(data) != digest || !validObject(data, ref) {
			return fmt.Errorf("existing immutable object is invalid")
		}
		return nil
	}()
}

func (r *Repository) ensureSession(key string) error {
	for _, dir := range []string{r.dir, filepath.Join(r.dir, repositorySessionsDir), r.sessionPath(key), filepath.Join(r.sessionPath(key), repositoryObjectsDir), filepath.Join(r.sessionPath(key), repositoryGenerations)} {
		if err := safedir.EnsurePrivate(dir); err != nil {
			return fmt.Errorf("create snapshot repository directory: %w", err)
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
func nameFromSafeKey(key string) string {
	if strings.HasPrefix(key, "@") {
		return ""
	}
	return key
}
func generationFilename(generation uint64) string {
	return fmt.Sprintf("%0*d.manifest", generationWidth, generation)
}
func parseGenerationFilename(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".manifest") || len(name) != generationWidth+len(".manifest") {
		return 0, false
	}
	n := strings.TrimSuffix(name, ".manifest")
	if strings.HasPrefix(n, "0") && n != fmt.Sprintf("%0*d", generationWidth, 0) { /* zero padding required, strconv below verifies width */
	}
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

func (r *Repository) atomicWrite(path string, data []byte) error {
	if r.hooks.writeFile != nil {
		return r.hooks.writeFile(path, append([]byte(nil), data...))
	}
	return atomicWriteFile(path, data)
}
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := safedir.EnsurePrivate(dir); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	cleanup = false
	return syncDirectory(dir)
}
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func readOptionalBounded(path string) ([]byte, bool, error) {
	data, err := readBounded(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return data, err == nil, err
}
func readBounded(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file")
	}
	if info.Size() < 0 || info.Size() > maxRepositoryRead {
		return nil, fmt.Errorf("snapshot file too large")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxRepositoryRead+1))
	if err != nil {
		return nil, err
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
