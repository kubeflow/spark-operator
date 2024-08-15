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
