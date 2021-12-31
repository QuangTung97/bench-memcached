package main

import "testing"

func BenchmarkMemcached(b *testing.B) {
	// benchMemcachedSet()
	benchMemcachedGetBatch()
}
