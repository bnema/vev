package brokerwire

// WithMaxStagedBytes overrides the staged estimate ceiling. It exists only for
// tests that prove the bound without allocating the production-sized default;
// production never needs a non-default ceiling.
func WithMaxStagedBytes(n uint64) SnapshotOption {
	return func(a *SnapshotAssembler) {
		a.maxStagedBytes = n
	}
}
