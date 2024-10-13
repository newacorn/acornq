package acornq

import _ "unsafe"

//go:noescape
//go:linkname nanoTime runtime.nanotime
func nanoTime() int64
