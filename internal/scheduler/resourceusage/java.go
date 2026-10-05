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

package resourceusage

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// DefaultUnit specifies the unit Spark uses when a memory value has no suffix.
// Spark uses MiB for most memory settings and bytes for spark.memory.offHeap.size.
type DefaultUnit int

const (
	// DefaultUnitMiB is used for spark.driver.memory, spark.executor.memory,
	// spark.driver.memoryOverhead, spark.executor.memoryOverhead, and
	// spark.executor.pyspark.memory.
	DefaultUnitMiB DefaultUnit = iota
	// DefaultUnitBytes is used for spark.memory.offHeap.size.
	DefaultUnitBytes
)

var (
	javaStringSuffixes = map[string]int64{
		"b":  1,
		"kb": 1 << 10,
		"k":  1 << 10,
		"mb": 1 << 20,
		"m":  1 << 20,
		"gb": 1 << 30,
		"g":  1 << 30,
		"tb": 1 << 40,
		"t":  1 << 40,
		"pb": 1 << 50,
		"p":  1 << 50,
	}

	// javaStringPattern matches an optional numeric part followed by an optional suffix.
	// A bare number (no suffix) is valid and handled via the defaultUnit parameter.
	javaStringPattern = regexp.MustCompile(`^([0-9]+)([a-z]+)?$`)
)

// byteStringAsBytes parses a Spark memory string (e.g. "1g", "512m", "1024") into bytes.
// When the string contains no unit suffix, defaultUnit determines the assumed unit:
//   - DefaultUnitMiB: bare numbers are treated as mebibytes (used for spark.driver.memory,
//     spark.executor.memory, spark.driver.memoryOverhead, spark.executor.memoryOverhead,
//     and spark.executor.pyspark.memory).
//   - DefaultUnitBytes: bare numbers are treated as bytes (used for spark.memory.offHeap.size).
func byteStringAsBytes(byteString string, defaultUnit DefaultUnit) (int64, error) {
	matches := javaStringPattern.FindStringSubmatch(strings.ToLower(byteString))
	if matches == nil {
		return 0, fmt.Errorf("unable to parse byte string: %s", byteString)
	}

	value, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, err
	}

	suffix := matches[2]
	if suffix == "" {
		// No suffix: apply the caller-specified default unit.
		switch defaultUnit {
		case DefaultUnitMiB:
			return value * (1 << 20), nil
		case DefaultUnitBytes:
			return value, nil
		default:
			return 0, fmt.Errorf("unknown default unit: %d", defaultUnit)
		}
	}

	if multiplier, present := javaStringSuffixes[suffix]; present {
		return value * multiplier, nil
	}
	return 0, fmt.Errorf("unable to parse byte string: %s", byteString)
}
