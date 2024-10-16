package acornq

import "unsafe"

func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
