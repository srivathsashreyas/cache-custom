package store

import (
	"errors"
	"strconv"
)

var errNotInteger = errors.New("value is not an integer or out of range")
var errIntOverflow = errors.New("value is not an integer or out of range")

func parseInt64(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errNotInteger
	}
	return n, nil
}

func formatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

func addInt64(a, b int64) (int64, bool) {
	c := a + b
	// overflow if signs same and result sign differs from a
	if (b > 0 && a > c) || (b < 0 && a < c) {
		return 0, false
	}
	return c, true
}
