package util

import (
	"hash"
	"hash/fnv"
)

// NewHash32 returns a 32-bit hash computed from the given byte slice.
func NewHash32() hash.Hash32 {
	return fnv.New32()
}
