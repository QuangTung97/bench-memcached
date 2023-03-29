package main

import "testing"

func BenchmarkMemcached(b *testing.B) {
	// benchMemcachedSet()
	for n := 0; n < b.N; n++ {
		benchMCGetBatchWithLatency()
	}
}

func TestDumpKeys(_ *testing.T) {
	dumpKeys()
}
