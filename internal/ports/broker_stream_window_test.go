package ports

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBrokerStreamWindowAdmission pins the bounded anti-replay window: a
// concurrent lower identity that was allocated first is admitted after a higher
// one, a duplicate inside the window is refused, an identity evicted past the
// bound is stale, and zero is never a valid identity.
func TestBrokerStreamWindowAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		admit   []BrokerStreamID
		wantErr []error
	}{
		{
			name:    "concurrent 2 then 1",
			admit:   []BrokerStreamID{2, 1},
			wantErr: []error{nil, nil},
		},
		{
			name:    "duplicate refused",
			admit:   []BrokerStreamID{2, 2},
			wantErr: []error{nil, BrokerAdmissionStale},
		},
		{
			name:    "abandoned identity stays consumable",
			admit:   []BrokerStreamID{3, 1, 2},
			wantErr: []error{nil, nil, nil},
		},
		{
			name:    "identity inside the window after a high one",
			admit:   []BrokerStreamID{BrokerStreamWindowSize, 1},
			wantErr: []error{nil, nil},
		},
		{
			name:    "identity evicted past the window is stale",
			admit:   []BrokerStreamID{1, BrokerStreamWindowSize + 1, 1},
			wantErr: []error{nil, nil, BrokerAdmissionStale},
		},
		{
			name:    "boundary identity still admitted",
			admit:   []BrokerStreamID{1, BrokerStreamWindowSize, 2},
			wantErr: []error{nil, nil, nil},
		},
		{
			name:    "zero identity refused",
			admit:   []BrokerStreamID{0},
			wantErr: []error{BrokerAdmissionInvalid},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var window BrokerStreamWindow
			for i, stream := range tc.admit {
				require.ErrorIs(t, window.Admit(stream), tc.wantErr[i], "admission %d of %d", i, stream)
			}
		})
	}
}

// TestBrokerStreamWindowConcurrentMonotone proves concurrent allocations admit
// every distinct identity exactly once and never collide, under the race
// detector.
func TestBrokerStreamWindowConcurrentMonotone(t *testing.T) {
	var window BrokerStreamWindow
	const count = 512
	var wg sync.WaitGroup
	errs := make([]error, count)
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = window.Admit(BrokerStreamID(i + 1))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "identity %d", i+1)
	}
	// Replaying one admitted identity is refused.
	require.ErrorIs(t, window.Admit(1), BrokerAdmissionStale)
}
