//go:build !goexperiment.runtimesecret || !linux || (!amd64 && !arm64)

package dgram

// SecretDo runs f synchronously. Without Go 1.26's experimental runtime secret
// support on linux/amd64 or linux/arm64, it invokes f directly. Runtime secret
// protection, when available, does not extend to global variables or goroutines
// started by f.
func SecretDo(f func()) {
	f()
}
