//go:build goexperiment.runtimesecret && linux && (amd64 || arm64)

package dgram

import "runtime/secret"

// SecretDo runs f synchronously. On supported platforms with Go 1.26's
// experimental runtime secret support enabled, it protects temporary storage
// used by f's full call tree. Protection does not extend to global variables or
// goroutines started by f.
func SecretDo(f func()) {
	secret.Do(f)
}
