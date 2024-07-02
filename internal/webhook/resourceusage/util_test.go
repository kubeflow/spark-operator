package resourceusage

import (
	"testing"
)

func assertMemory(memoryString string, expectedBytes int64, t *testing.T) {
	m, err := parseJavaMemoryString(memoryString)
	if err != nil {
		t.Error(err)
		return
	}
	if m != expectedBytes {
		t.Errorf("%s: expected %v bytes, got %v bytes", memoryString, expectedBytes, m)
		return
	}
}

func TestJavaMemoryString(t *testing.T) {
	assertMemory("1b", 1, t)
	assertMemory("100k", 100*1024, t)
	assertMemory("1gb", 1024*1024*1024, t)
	assertMemory("10TB", 10*1024*1024*1024*1024, t)
	assertMemory("10PB", 10*1024*1024*1024*1024*1024, t)
}
