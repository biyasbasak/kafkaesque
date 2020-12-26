package utils

import "hash/fnv"

// Ihash takes a key and a range.
//
func Ihash(key string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % n
}
