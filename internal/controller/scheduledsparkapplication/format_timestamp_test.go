package scheduledsparkapplication

import (
	"testing"
	"time"
)

func TestFormatTimestampLengths(t *testing.T) {
	// deterministic time so tests are stable
	now := time.Unix(1700000000, 123456789) // arbitrary fixed timestamp

	cases := map[string]int{
		"minutes": 8,  // 1700000000 / 60 ~= 28333333 (8 digits)
		"seconds": 10, // 1700000000 -> 10 digits
		"millis":  13,
		"micros":  16,
		"nanos":   19,
	}

	for precision, wantLen := range cases {
		s := formatTimestamp(precision, now)
		if len(s) != wantLen {
			t.Fatalf("precision=%s: got len %d, want %d (value=%s)", precision, len(s), wantLen, s)
		}
	}
}
