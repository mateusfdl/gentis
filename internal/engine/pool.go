package engine

// getSubscriberSlice allocates a new subscriber slice with the requested
// capacity. The returned slice has length 0.
//
// Previous versions used sync.Pool-based size-class pooling, but because
// old subscriber slices cannot be returned to the pool (concurrent
// publishers may still be iterating them via the copy-on-write pattern),
// the pool was effectively write-only and provided no reuse benefit.
// A direct make() is simpler and avoids the indirection overhead.
func getSubscriberSlice(need int) []SubscriberID {
	return make([]SubscriberID, 0, need)
}
