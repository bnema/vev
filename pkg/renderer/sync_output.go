package renderer

const (
	SyncStartCSI = "\x1b[?2026h"
	SyncEndCSI   = "\x1b[?2026l"
)

func WrapSynchronized(content []byte, enabled bool) []byte {
	if !enabled || len(content) == 0 {
		return content
	}
	out := make([]byte, 0, len(SyncStartCSI)+len(content)+len(SyncEndCSI))
	out = append(out, SyncStartCSI...)
	out = append(out, content...)
	out = append(out, SyncEndCSI...)
	return out
}
