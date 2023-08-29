package main

import "testing"

func BenchmarkMemcached(b *testing.B) {
	// benchMemcachedSet()
	for n := 0; n < 10; n++ {
		benchMyMemcacheClientGetBatch()
		// benchMemcachedGetBatch()
	}
}

func TestDumpKeys(_ *testing.T) {
	dumpKeys()
}
