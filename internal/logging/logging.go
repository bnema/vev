package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/bnema/vev/pkg/safedir"
)

type Component string

const (
	Daemon Component = "daemon"
	Client Component = "client"
	Stdio  Component = "stdio"
)

const DefaultMaxBytes = 20 << 20

type Config struct {
	Dir             string
	Component       Component
	Level           slog.Level
	MaxBytes        int64
	RotateAtRuntime bool
}

func Setup(cfg Config) (*slog.Logger, io.Closer, error) {
	if err := safedir.EnsurePrivate(cfg.Dir); err != nil {
		return nil, nil, err
	}

	component := cfg.Component
	logPath := filepath.Join(cfg.Dir, "vev-"+string(component)+".log")
	writer, err := newRotatingWriter(logPath, cfg.MaxBytes, cfg.RotateAtRuntime)
	if err != nil {
		return nil, nil, err
	}

	crashPath := filepath.Join(cfg.Dir, "vev-"+string(component)+"-crash.log")
	crashFile, err := os.OpenFile(crashPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = writer.Close()
		return nil, nil, err
	}
	if err := debug.SetCrashOutput(crashFile, debug.CrashOptions{}); err != nil {
		_ = crashFile.Close()
		_ = writer.Close()
		return nil, nil, err
	}

	logger := slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: cfg.Level})).With("component", string(component))
	slog.SetDefault(logger)

	return logger, &setupCloser{log: writer, crash: crashFile}, nil
}

func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func EnvLevel() slog.Level {
	return ParseLevel(os.Getenv("VEV_LOG"))
}

type setupCloser struct {
	once  sync.Once
	log   io.Closer
	crash io.Closer
	err   error
}

func (c *setupCloser) Close() error {
	c.once.Do(func() {
		if c.log != nil {
			c.err = c.log.Close()
		}
		if c.crash != nil {
			if err := c.crash.Close(); c.err == nil {
				c.err = err
			}
		}
	})
	return c.err
}
