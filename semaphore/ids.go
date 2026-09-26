package semaphore

import "strings"

// joinIDs / splitIDs pack a list of holder ids into one request field.
func joinIDs(ids []string) string { return strings.Join(ids, ",") }

func splitIDs(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitDash parses "<prefix>-<instance>-<random>". Instance ids themselves
// must not contain '-' (validated implicitly by the first two segments).
func splitDash(id string) []string {
	return strings.SplitN(id, "-", 3)
}
