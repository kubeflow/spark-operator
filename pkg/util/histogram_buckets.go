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
