package brokeripc

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"runtime/debug"
	"sync"
)

var defaultBuildIdentity = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	version, revision, modified := "", "", false
	if ok {
		version = info.Main.Version
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
	}
	if revision != "" && !modified {
		return version + "+" + revision
	}
	path, err := os.Executable()
	if err == nil {
		file, openErr := os.Open(path)
		if openErr == nil {
			defer file.Close()
			hash := sha256.New()
			if _, hashErr := io.Copy(hash, file); hashErr == nil {
				return "sha256:" + hex.EncodeToString(hash.Sum(nil)[:16])
			}
		}
	}
	return version + "+" + revision + "-dirty"
})
