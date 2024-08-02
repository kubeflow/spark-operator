package yunikorn

import "maps"

func mergeMaps(m1, m2 map[string]string) map[string]string {
	out := make(map[string]string)

	maps.Copy(out, m1)
	maps.Copy(out, m2)

	// Return nil if there are no keys so the struct field is skipped JSON marshalling
	if len(out) == 0 {
		return nil
	}
	return out
}
