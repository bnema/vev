//go:build !goexperiment.runtimesecret

package dgram

// SecretDo invokes f directly when runtime/secret is not in this build.
// Callers still explicitly clear every reachable key buffer with Erase.
func SecretDo(f func()) { f() }
