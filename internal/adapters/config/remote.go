package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/safedir"
)

func parseRemoteKey(remote domain.RemoteConfig, seen map[string]bool, key, value string, lineNo int) (domain.RemoteConfig, []domain.Warning) {
	var warnings []domain.Warning
	switch key {
	case "enabled", "remember", "hosts":
		warnings = warnDuplicateKey(warnings, seen, key, lineNo)
	default:
		warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("unknown key %q", key)})
		return remote, warnings
	}

	switch key {
	case "enabled":
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid enabled %q", value)})
			return remote, warnings
		}
		remote.Enabled = on
	case "remember":
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid remember %q", value)})
			return remote, warnings
		}
		remote.Remember = on
	case "hosts":
		hosts, hostWarnings := parseRemoteHosts(value, lineNo)
		warnings = append(warnings, hostWarnings...)
		if hosts == nil {
			return remote, warnings
		}
		remote.Hosts = hosts
	}
	return remote, warnings
}

func parseTrueFalse(value string) (bool, bool) {
	switch strings.TrimSpace(value) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

func parseRemoteHosts(value string, lineNo int) ([]string, []domain.Warning) {
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(value), &raw); err != nil {
		return nil, []domain.Warning{{Line: lineNo, Msg: fmt.Sprintf("invalid hosts %q", value)}}
	}
	if len(raw) == 0 || raw[0] != '[' {
		return nil, []domain.Warning{{Line: lineNo, Msg: fmt.Sprintf("invalid hosts %q", value)}}
	}
	var items []any
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, []domain.Warning{{Line: lineNo, Msg: fmt.Sprintf("invalid hosts %q", value)}}
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	var warnings []domain.Warning
	ok := true
	for _, item := range items {
		host, isString := item.(string)
		if !isString {
			return nil, []domain.Warning{{Line: lineNo, Msg: fmt.Sprintf("invalid hosts %q", value)}}
		}
		if err := domain.ValidateRemoteHostTarget(host); err != nil {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid remote host %q: %v", host, err)})
			ok = false
			continue
		}
		if _, dup := seen[host]; dup {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("duplicate host %q", host)})
			ok = false
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	if !ok {
		return nil, warnings
	}
	return out, nil
}

// UpdateRemoteHosts creates or replaces the pinned hosts list in path's [remote]
// section while preserving unrelated config text. Writes use a private parent
// directory and same-directory temp + sync + rename + directory sync.
func UpdateRemoteHosts(path string, hosts []string) error {
	hosts = domain.UniqueRemoteHostTargets(hosts)
	for _, host := range hosts {
		if err := domain.ValidateRemoteHostTarget(host); err != nil {
			return err
		}
	}
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return err
	}

	lockFile, err := acquireConfigLock(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = releaseConfigLock(lockFile)
	}()

	original, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	missing := errors.Is(err, os.ErrNotExist)

	hostsLine, err := formatRemoteHostsLine(hosts)
	if err != nil {
		return err
	}

	var next []byte
	if missing || len(bytes.TrimSpace(original)) == 0 {
		next = []byte("[remote]\n" + hostsLine + "\n")
	} else {
		next = rewriteRemoteHosts(original, hostsLine)
	}
	return writeConfigAtomic(path, next)
}

func formatRemoteHostsLine(hosts []string) (string, error) {
	var b strings.Builder
	b.WriteString("hosts = [")
	for i, host := range hosts {
		if i > 0 {
			b.WriteString(", ")
		}
		encoded, err := json.Marshal(host)
		if err != nil {
			return "", err
		}
		b.Write(encoded)
	}
	b.WriteByte(']')
	return b.String(), nil
}

func rewriteRemoteHosts(original []byte, hostsLine string) []byte {
	lines := splitKeepNewlines(original)
	remoteIdx := -1
	hostsIdx := -1
	inRemote := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"))
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		trimmed = stripInlineComment(trimmed)
		if matches := sectionPattern.FindStringSubmatch(trimmed); matches != nil {
			inRemote = matches[1] == "remote"
			if inRemote && remoteIdx < 0 {
				remoteIdx = i
			}
			continue
		}
		if !inRemote {
			continue
		}
		key, _, ok := strings.Cut(trimmed, "=")
		if !ok {
			continue
		}
		if strings.TrimSpace(key) == "hosts" {
			hostsIdx = i
		}
	}

	ending := dominantLineEnding(original)

	switch {
	case hostsIdx >= 0:
		prefix := lines[hostsIdx]
		nl := "\n"
		if strings.HasSuffix(prefix, "\r\n") {
			nl = "\r\n"
		} else if !strings.HasSuffix(prefix, "\n") {
			nl = ""
		}
		lines[hostsIdx] = hostsLine + nl
	case remoteIdx >= 0:
		insertAt := remoteIdx + 1
		for insertAt < len(lines) {
			trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(lines[insertAt], "\n"), "\r"))
			if matches := sectionPattern.FindStringSubmatch(stripInlineComment(trimmed)); matches != nil {
				break
			}
			insertAt++
		}
		hostsEntry := hostsLine + ending
		lines = append(lines[:insertAt], append([]string{hostsEntry}, lines[insertAt:]...)...)
	default:
		if len(lines) > 0 {
			last := lines[len(lines)-1]
			if last != "" && !strings.HasSuffix(last, "\n") && !strings.HasSuffix(last, "\r\n") {
				lines[len(lines)-1] = last + ending
			}
		}
		lines = append(lines, "[remote]"+ending, hostsLine+ending)
	}

	var b strings.Builder
	for _, line := range lines {
		b.WriteString(line)
	}
	return []byte(b.String())
}

func dominantLineEnding(data []byte) string {
	crlf := 0
	lf := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '\n' {
			continue
		}
		if i > 0 && data[i-1] == '\r' {
			crlf++
		} else {
			lf++
		}
	}
	if crlf > lf {
		return "\r\n"
	}
	return "\n"
}

func acquireConfigLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func releaseConfigLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}

func splitKeepNewlines(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	var lines []string
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			lines = append(lines, string(data[start:i+1]))
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, string(data[start:]))
	}
	return lines
}

func writeConfigAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".vev-config-*.tmp")
	if err != nil {
		return err
	}
	tmp := file.Name()
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
			_ = os.Remove(tmp)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	committed = true
	return syncDir(dir)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}
