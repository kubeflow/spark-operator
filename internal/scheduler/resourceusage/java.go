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

	javaStringPattern = regexp.MustCompile(`^([0-9]+)([a-z]+)?$`)
)

func byteStringAsBytes(byteString string) (int64, error) {
	matches := javaStringPattern.FindStringSubmatch(strings.ToLower(byteString))
	if matches != nil {
		value, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, err
		}
		if multiplier, present := javaStringSuffixes[matches[2]]; present {
			return value * multiplier, nil
		}
	}
	return 0, fmt.Errorf("unable to parse byte string: %s", byteString)
}
