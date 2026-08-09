// Package vtcore defines the frontend-neutral terminal model shared by the VT
// emulator and concrete ANSI renderers.
//
// Frame and other mutable values are single-owner. Frame.Row returns a backing
// slice that is valid until the next scroll or resize; Frame.Clone returns
// independent storage. The package has no frontend, transport, or vev policy.
package vtcore
