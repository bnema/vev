package snapshot

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const maxSnapshotFileSize = 16 + (256 << 20)

func incarnationKey(id domain.IncarnationID) (string, error) {
	if id == (domain.IncarnationID{}) {
		return "", fmt.Errorf("snapshot: zero incarnation ID")
	}
	return id.String(), nil
}

func (r *Repository) sessionPath(id domain.IncarnationID) string {
	return filepath.Join(r.dir, repositorySessionsDir, id.String())
}
func (r *Repository) objectPath(id domain.IncarnationID, digest ports.SnapshotDigest) string {
	hexDigest := hex.EncodeToString(digest[:])
	return filepath.Join(r.sessionPath(id), repositoryObjectsDir, hexDigest[:2], hexDigest)
}
func (r *Repository) manifestPath(id domain.IncarnationID, generation uint64) string {
	return filepath.Join(r.sessionPath(id), repositoryGenerations, generationFilename(generation))
}
func (r *Repository) headPath(id domain.IncarnationID) string {
	return filepath.Join(r.sessionPath(id), repositoryHead)
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

func parseObjectDigest(name string) (ports.SnapshotDigest, bool) {
	var digest ports.SnapshotDigest
	if len(name) != hex.EncodedLen(len(digest)) || strings.ToLower(name) != name {
		return digest, false
	}
	_, err := hex.Decode(digest[:], []byte(name))
	return digest, err == nil
}
