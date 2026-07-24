//go:build goexperiment.runtimesecret

package dgram

import "runtime/secret"

// SecretDo invokes f through runtime/secret.Do. On supported Go 1.26
// platforms this protects temporary stack/register storage for f's call tree
// and marks its allocations for clearing after they become unreachable and
// the GC notices. On unsupported platforms runtime/secret.Do invokes f
// directly. It does not protect globals or goroutines started by f.
func SecretDo(f func()) { secret.Do(f) }
