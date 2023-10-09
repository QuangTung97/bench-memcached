package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkMemcached(b *testing.B) {
	// benchMemcachedSet()
	for n := 0; n < 10; n++ {
		benchMyMemcacheClientGetBatch()
		// benchMemcachedGetBatch()
	}
}

func TestComputeKey(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		assert.Equal(t, "KEY0000009", computeKey(8))

		assert.Equal(t, "KEY0001284", computeKey(1283))

		assert.Equal(t, "KEY0000001", computeKey(0))
	})
}

func TestDumpKeys(_ *testing.T) {
	dumpKeys()
}
