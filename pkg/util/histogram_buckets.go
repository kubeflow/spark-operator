/*
Copyright 2018 Google LLC

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

package util

import (
	"fmt"
	"strconv"
	"strings"
)

var DefaultJobStartLatencyBuckets = []float64{30, 60, 90, 120, 150, 180, 210, 240, 270, 300}

type HistogramBuckets []float64

func (hb *HistogramBuckets) String() string {
	return fmt.Sprint(*hb)
}

func (hb *HistogramBuckets) Set(value string) error {
	*hb = nil
	for _, boundaryStr := range strings.Split(value, ",") {
		boundary, err := strconv.ParseFloat(strings.TrimSpace(boundaryStr), 64)
		if err != nil {
			return err
		}
		*hb = append(*hb, boundary)
	}
	return nil
}
