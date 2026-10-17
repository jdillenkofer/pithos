package object

import "unicode/utf8"

// PrefixUpperBound returns the exclusive upper bound of a UTF-8 prefix in
// bytewise key order. Together with key >= prefix, key < this bound selects
// exactly the keys starting with prefix, without SQL pattern matching.
// An empty bound means no upper bound is needed (the prefix is empty or consists
// entirely of the largest Unicode scalar value).
func PrefixUpperBound(prefix string) string {
	// Object keys must be valid UTF-8, so an invalid prefix cannot match any key.
	// An upper bound equal to the prefix makes the range empty.
	if !utf8.ValidString(prefix) {
		return prefix
	}
	for end := len(prefix); end > 0; {
		last, size := utf8.DecodeLastRuneInString(prefix[:end])
		end -= size
		if last == utf8.MaxRune {
			continue
		}
		next := last + 1
		if next == 0xD800 {
			// Surrogates are not Unicode scalar values and cannot occur in keys.
			next = 0xE000
		}
		return prefix[:end] + string(next)
	}
	return ""
}
