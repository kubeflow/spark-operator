/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

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
	// No suffix means MiB, as for spark.{driver,executor}.memory and memoryOverhead.
	assertMemory("384", 384*1024*1024, t)
	assertMemory(" 2048 ", 2048*1024*1024, t)
}

func TestJavaMemoryStringInvalid(t *testing.T) {
	for _, s := range []string{"", "g", "1.5g", "0.5", "1g1", "10gx", "-1g", "1 g"} {
		if m, err := parseJavaMemoryString(s); err == nil {
			t.Errorf("%q: expected an error, got %v bytes", s, m)
		}
	}
}
