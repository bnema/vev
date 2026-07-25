package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

var safeNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

const maxSnapshotFileSize = 16 + (256 << 20)

// filenameForName maps a session name to its deterministic legacy filename.
// It remains only for the one-way v3 import path.
func filenameForName(name string) string {
	if safeNameRE.MatchString(name) {
		return name + ".snap"
	}
	sum := sha256.Sum256([]byte(name))
	return "@" + hex.EncodeToString(sum[:])[:40] + ".snap"
}

func incarnationKey(id domain.IncarnationID) (string, error) {
	if id == (domain.IncarnationID{}) {
		return "", fmt.Errorf("snapshot: zero incarnation ID")
	}
	return id.String(), nil
}

// repositoryKey accepts string only for pre-migration maintenance code. Normal
// publication and loading always pass an IncarnationID.
func repositoryKey(identity any) string {
	switch value := identity.(type) {
	case domain.IncarnationID:
		return value.String()
	case string:
		return value
	default:
		panic("snapshot: invalid repository identity")
	}
}

func (r *Repository) sessionPath(identity any) string {
	return filepath.Join(r.dir, repositorySessionsDir, repositoryKey(identity))
}
func (r *Repository) objectPath(identity any, digest ports.SnapshotDigest) string {
	hexDigest := hex.EncodeToString(digest[:])
	return filepath.Join(r.sessionPath(identity), repositoryObjectsDir, hexDigest[:2], hexDigest)
}
func (r *Repository) manifestPath(identity any, generation uint64) string {
	return filepath.Join(r.sessionPath(identity), repositoryGenerations, generationFilename(generation))
}
func (r *Repository) headPath(identity any) string {
	return filepath.Join(r.sessionPath(identity), repositoryHead)
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

func parseObjectDigest(name string) (ports.SnapshotDigest, bool) {
	var digest ports.SnapshotDigest
	if len(name) != hex.EncodedLen(len(digest)) || strings.ToLower(name) != name {
		return digest, false
	}
	_, err := hex.Decode(digest[:], []byte(name))
	return digest, err == nil
}
