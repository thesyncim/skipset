package skipset

import (
	"sync/atomic"
	"unsafe"
)

const (
	op1 = 4
	op2 = maxLevel - op1
)

type optionalArray struct {
	Base  [op1]unsafe.Pointer
	Extra *[op2]unsafe.Pointer
}

func (a *optionalArray) Load(i int) unsafe.Pointer {
	if i < op1 {
		return a.Base[i]
	}
	return a.Extra[i-op1]
}

func (a *optionalArray) Store(i int, p unsafe.Pointer) {
	if i < op1 {
		a.Base[i] = p
		return
	}
	a.Extra[i-op1] = p
}

func (a *optionalArray) AtomicLoad(i int) unsafe.Pointer {
	if i < op1 {
		return atomic.LoadPointer(&a.Base[i])
	}
	return atomic.LoadPointer(&a.Extra[i-op1])
}

func (a *optionalArray) AtomicStore(i int, p unsafe.Pointer) {
	if i < op1 {
		atomic.StorePointer(&a.Base[i], p)
		return
	}
	atomic.StorePointer(&a.Extra[i-op1], p)
}
