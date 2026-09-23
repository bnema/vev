package quic

import "github.com/bnema/vev/internal/ports"

// WithRuntimeObserver enables process-local adapter marks on the dialed
// transport, for tests that assert on those marks. Production never needs
// runtime observation on a QUIC dial.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(opts *dialOptions) { opts.observer = observer }
}
