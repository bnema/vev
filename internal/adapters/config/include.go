package config

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bnema/kvconf"
	"github.com/bnema/vev/internal/domain"
)

const maxIncludes = 16

type configSource struct {
	file string
	line int
}
type expandedConfig struct {
	data     bytes.Buffer
	sources  []configSource
	warnings []domain.Warning
	paths    map[string]bool
}

// expandIncludes accepts directives only in the main file. Every source is
// individually bounded by kvconf's file limit; at most 16 files are inserted.
func expandIncludes(r io.Reader, file string) (*expandedConfig, error) {
	data, err := io.ReadAll(io.LimitReader(r, kvconf.DefaultMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > kvconf.DefaultMaxBytes {
		return nil, kvconf.ErrTooLarge
	}
	result := &expandedConfig{paths: make(map[string]bool)}
	if file != "" {
		result.paths[file] = true
	}
	base := "."
	if file != "" {
		base = filepath.Dir(file)
	}
	count := 0
	var appendSource func([]byte, string, bool) error
	appendSource = func(data []byte, name string, main bool) error {
		scanner := bufio.NewScanner(bytes.NewReader(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))))
		scanner.Buffer(make([]byte, 4096), kvconf.DefaultMaxBytes+1)
		lineNo := 0
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			source := configSource{name, lineNo}
			fields := strings.Fields(line)
			if len(fields) == 0 || fields[0] != "include" {
				result.data.WriteString(line)
				result.data.WriteByte('\n')
				result.sources = append(result.sources, source)
				continue
			}
			warn := func(msg string) {
				result.warnings = append(result.warnings, sourceWarning(domain.Warning{Line: lineNo, Msg: msg}, source))
			}
			if !main {
				warn("nested include is not supported")
				continue
			}
			count++
			if count > maxIncludes {
				warn("config include limit of 16 exceeded")
				continue
			}
			entries, warnings, err := kvconf.Parse(strings.NewReader("path = "+strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "include"))), parseOptions)
			if err != nil {
				return err
			}
			if len(warnings) != 0 || len(entries) != 1 {
				warn("invalid include")
				continue
			}
			path, ok := parseConfigString(entries[0].Value)
			if !ok || path == "" {
				warn("invalid include")
				continue
			}
			if strings.HasPrefix(path, "~/") {
				home, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				path = filepath.Join(home, path[2:])
			}
			if !filepath.IsAbs(path) {
				path = filepath.Join(base, path)
			}
			path, err = filepath.Abs(path)
			if err != nil {
				return err
			}
			result.paths[path] = true
			included, err := kvconf.ReadFile(path, parseOptions)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if err := appendSource(included, path, false); err != nil {
				return err
			}
		}
		return scanner.Err()
	}
	err = appendSource(data, file, true)
	return result, err
}

func sourceWarning(w domain.Warning, source configSource) domain.Warning {
	w.Line = source.line
	if source.file != "" {
		w.Msg = fmt.Sprintf("%s:%d: %s", source.file, source.line, w.Msg)
	}
	return w
}

func readConfigIncludes(path string) (*expandedConfig, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	data, err := kvconf.ReadFile(path, parseOptions)
	if err != nil {
		return &expandedConfig{paths: map[string]bool{path: true}}, err
	}
	return expandIncludes(bytes.NewReader(data), path)
}

// watchConfigFiles installs each dependency watch, waits for its readiness,
// then rescans before publishing ready or a change. A bounded reconciliation
// prevents a concurrently rewritten main file from starving cancellation.
func watchConfigFiles(ctx context.Context, path string, opts kvconf.WatchOptions, out chan<- struct{}) error {
	ctx, cancelAll := context.WithCancel(ctx)
	changed := make(chan struct{}, 1)
	failed := make(chan error, 1)
	watches := make(map[string]context.CancelFunc)
	var wg sync.WaitGroup
	defer func() { cancelAll(); wg.Wait() }()
	reconcile := func() error {
		for attempt := 0; attempt < maxIncludes+2; attempt++ {
			config, _ := readConfigIncludes(path)
			if config == nil {
				return fmt.Errorf("cannot resolve config path %q", path)
			}
			paths := config.paths
			for p, cancel := range watches {
				if !paths[p] {
					cancel()
					delete(watches, p)
				}
			}
			var readiness []chan struct{}
			for p := range paths {
				if _, ok := watches[p]; ok {
					continue
				}
				child, cancel := context.WithCancel(ctx)
				watches[p] = cancel
				options := opts
				ready := make(chan struct{})
				options.Ready = ready
				readiness = append(readiness, ready)
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := kvWatch(child, p, options, changed)
					if child.Err() == nil {
						if err == nil {
							err = errors.New("config watcher stopped")
						}
						select {
						case failed <- err:
						default:
						}
					}
				}()
			}
			for _, ready := range readiness {
				select {
				case <-ready:
				case err := <-failed:
					return err
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// A file may have changed between discovering it and watch installation.
			// The caller reloads after this rescan even if no native event was seen.
			next, _ := readConfigIncludes(path)
			if next != nil && maps.Equal(paths, next.paths) {
				return nil
			}
		}
		return errors.New("config includes did not stabilize during watch installation")
	}
	if err := reconcile(); err != nil {
		return err
	}
	if opts.Ready != nil {
		close(opts.Ready)
	}
	// Also reload once after initial installation to cover its read/watch gap.
	select {
	case out <- struct{}{}:
	default:
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-failed:
			return err
		case <-changed:
			if err := reconcile(); err != nil {
				return err
			}
			select {
			case out <- struct{}{}:
			default:
			}
		}
	}
}
