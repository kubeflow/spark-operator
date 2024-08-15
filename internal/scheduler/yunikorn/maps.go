package yunikorn

import (
	"maps"
)

func mergeMaps(m1, m2 map[string]string) map[string]string {
	out := make(map[string]string)

	maps.Copy(out, m1)
	maps.Copy(out, m2)

	// Return nil if there are no entries in the map so that the field is skipped during JSON marshalling
	if len(out) == 0 {
		return nil
	}
	return out
}
