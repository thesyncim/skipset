package skipset

import (

	// required for linkname
	_ "unsafe"
)

const (
	maxLevel            = 16
	p                   = 0.25
	defaultHighestLevel = 3
)

//go:linkname runtimefastrand runtime.fastrand
func runtimefastrand() uint32

// uint32n returns a pseudo-random number in [0,n).
//go:nosplit
func uint32n(n uint32) uint32 {
	// This is similar to Uint32() % n, but faster.
	// See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return uint32(uint64(runtimefastrand()) * uint64(n) >> 32)
}

func randomLevel() int {
	level := 1
	for uint32n(1/p) == 0 {
		level++
	}
	if level > maxLevel {
		return maxLevel
	}
	return level
}
