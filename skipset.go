package skipset

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// SkipSet represents a generic set based on skip list
type SkipSet[T any] struct {
	less         func(a, b T) bool
	header       *node[T]
	length       int64
	highestLevel int64
}

func (s *SkipSet[T]) equal(a, b T) bool {
	return !s.less(a, b) && !s.less(b, a)
}

// New return an empty Item the set in ascending order.
func New[T any](lessFunc func(a, b T) bool) *SkipSet[T] {
	var t T
	h := newNode[T](t, maxLevel)
	h.flags.SetTrue(fullyLinked)
	return &SkipSet[T]{
		header:       h,
		highestLevel: defaultHighestLevel,
		less:         lessFunc,
	}
}

// findNodeRemove takes a value and two maximal-height arrays then searches exactly as in a sequential skip-list.
// The returned preds and succs always satisfy preds[i] > value >= succs[i].
func (s *SkipSet[T]) findNodeRemove(value T, preds, succs *[maxLevel]*node[T]) int {
	// lFound represents the index of the first layer at which it found a node.
	lFound, x := -1, s.header
	for i := int(atomic.LoadInt64(&s.highestLevel)) - 1; i >= 0; i-- {
		succ := x.atomicLoadNext(i)
		for succ != nil && s.less(succ.value, value) {
			x = succ
			succ = x.atomicLoadNext(i)
		}
		preds[i] = x
		succs[i] = succ

		// Check if the value already in the skip list.
		if lFound == -1 && succ != nil && s.equal(succ.value, value) {
			lFound = i
		}
	}
	return lFound
}

// findNodeAdd takes a value and two maximal-height arrays then searches exactly as in a sequential skip-set.
// The returned preds and succs always satisfy preds[i] > value >= succs[i].
func (s *SkipSet[T]) findNodeAdd(value T, preds, succs *[maxLevel]*node[T]) int {
	x := s.header
	for i := int(atomic.LoadInt64(&s.highestLevel)) - 1; i >= 0; i-- {
		succ := x.atomicLoadNext(i)
		for succ != nil && s.less(succ.value, value) {
			x = succ
			succ = x.atomicLoadNext(i)
		}
		preds[i] = x
		succs[i] = succ

		// Check if the value already in the skip list.
		if succ != nil && s.equal(succ.value, value) {
			return i
		}
	}
	return -1
}

// findNodeAdd takes a value and two maximal-height arrays then searches exactly as in a sequential skip-set.
// The returned preds and succs always satisfy preds[i] > value >= succs[i].
func (s *SkipSet[T]) findNode(value T, preds, succs *[maxLevel]*node[T]) *node[T] {
	x := s.header
	for i := int(atomic.LoadInt64(&s.highestLevel)) - 1; i >= 0; i-- {
		succ := x.atomicLoadNext(i)
		for succ != nil && s.less(succ.value, value) {
			x = succ
			succ = x.atomicLoadNext(i)
		}
		preds[i] = x
		succs[i] = succ

		// Check if the value already in the skip list.
		if succ != nil && s.equal(succ.value, value) {
			return succ
		}
	}
	return nil
}

func unlockNode[T any](preds [maxLevel]*node[T], highestLevel int) {
	var prevPred *node[T]
	for i := highestLevel; i >= 0; i-- {
		if preds[i] != prevPred { // the node could be unlocked by previous loop
			preds[i].mu.Unlock()
			prevPred = preds[i]
		}
	}
}

// Store adds the value into the set, return true if this process insert the value into the set,
// return false if this process can't insert this value, because another process has inserted the same value.
//
// If the value is in the set but not fully linked, this process will wait until it is.
func (s *SkipSet[T]) Store(value T) bool {
	level := s.randomlevel()
	var preds, succs [maxLevel]*node[T]
	for {
		lFound := s.findNodeAdd(value, &preds, &succs)
		if lFound != -1 { // indicating the value is already in the skip-list
			nodeFound := succs[lFound]
			if !nodeFound.flags.Get(marked) {
				for !nodeFound.flags.Get(fullyLinked) {
					// The node is not yet fully linked, just waits until it is.
				}
				return false
			}
			// If the node is marked, represents some other thread is in the process of deleting this node,
			// we need to add this node in next loop.
			continue
		}
		// Store this node into skip list.
		var (
			highestLocked        = -1 // the highest level being locked by this process
			valid                = true
			pred, succ, prevPred *node[T]
		)
		for layer := 0; valid && layer < level; layer++ {
			pred = preds[layer]   // target node's previous node
			succ = succs[layer]   // target node's next node
			if pred != prevPred { // the node in this layer could be locked by previous loop
				pred.mu.Lock()
				highestLocked = layer
				prevPred = pred
			}
			// valid check if there is another node has inserted into the skip list in this layer during this process.
			// It is valid if:
			// 1. The previous node and next node both are not marked.
			// 2. The previous node's next node is succ in this layer.
			valid = !pred.flags.Get(marked) && (succ == nil || !succ.flags.Get(marked)) && pred.loadNext(layer) == succ
		}
		if !valid {
			unlockNode(preds, highestLocked)
			continue
		}

		nn := newNode(value, level)
		for layer := 0; layer < level; layer++ {
			nn.storeNext(layer, succs[layer])
			preds[layer].atomicStoreNext(layer, nn)
		}
		nn.flags.SetTrue(fullyLinked)
		unlockNode(preds, highestLocked)
		atomic.AddInt64(&s.length, 1)
		return true
	}
}

// Set is similar to store but ensures given value is stored into the set.
func (s *SkipSet[T]) Set(value T) {
	level := s.randomlevel()
	var preds, succs [maxLevel]*node[T]
	for {
		lFound := s.findNodeAdd(value, &preds, &succs)
		if lFound != -1 { // indicating the value is already in the skip-list
			nodeFound := succs[lFound]
			if !nodeFound.flags.Get(marked) {
				for !nodeFound.flags.Get(fullyLinked) {
					// The node is not yet fully linked, just waits until it is.
				}
				s.Remove(value)
			}
			// If the node is marked, represents some other thread is in the process of deleting this node,
			// we need to add this node in next loop.
			continue
		}
		// Store this node into skip list.
		var (
			highestLocked        = -1 // the highest level being locked by this process
			valid                = true
			pred, succ, prevPred *node[T]
		)
		for layer := 0; valid && layer < level; layer++ {
			pred = preds[layer]   // target node's previous node
			succ = succs[layer]   // target node's next node
			if pred != prevPred { // the node in this layer could be locked by previous loop
				pred.mu.Lock()
				highestLocked = layer
				prevPred = pred
			}
			// valid check if there is another node has inserted into the skip list in this layer during this process.
			// It is valid if:
			// 1. The previous node and next node both are not marked.
			// 2. The previous node's next node is succ in this layer.
			valid = !pred.flags.Get(marked) && (succ == nil || !succ.flags.Get(marked)) && pred.loadNext(layer) == succ
		}
		if !valid {
			unlockNode(preds, highestLocked)
			continue
		}

		nn := newNode(value, level)
		for layer := 0; layer < level; layer++ {
			nn.storeNext(layer, succs[layer])
			preds[layer].atomicStoreNext(layer, nn)
		}
		nn.flags.SetTrue(fullyLinked)
		unlockNode(preds, highestLocked)
		atomic.AddInt64(&s.length, 1)
		return
	}
}

func (s *SkipSet[T]) randomlevel() int {
	// Generate random level.
	level := randomLevel()
	// Update highest level if possible.
	for {
		hl := atomic.LoadInt64(&s.highestLevel)
		if int64(level) <= hl {
			return level
		}
		if atomic.CompareAndSwapInt64(&s.highestLevel, hl, int64(level)) {
			return level
		}
	}
}

// Contains check if the value is in the the set.
func (s *SkipSet[T]) Contains(value T) bool {
	x := s.header
	for i := int(atomic.LoadInt64(&s.highestLevel)) - 1; i >= 0; i-- {
		nex := x.atomicLoadNext(i)
		for nex != nil && s.less(nex.value, value) {
			x = nex
			nex = x.atomicLoadNext(i)
		}

		// Check if the value already in the skip list.
		if nex != nil && s.equal(nex.value, value) {
			return nex.flags.MGet(fullyLinked|marked, fullyLinked)
		}
	}
	return false
}

// Load returns the value in the set
func (s *SkipSet[T]) Load(value T) (v T, found bool) {
	x := s.header
	for i := int(atomic.LoadInt64(&s.highestLevel)) - 1; i >= 0; i-- {
		nex := x.atomicLoadNext(i)
		for nex != nil && s.less(nex.value, value) {
			x = nex
			nex = x.atomicLoadNext(i)
		}

		// Check if the value already in the skip list.
		if nex != nil && s.equal(nex.value, value) {
			linked := nex.flags.MGet(fullyLinked|marked, fullyLinked)
			if linked {
				return nex.value, true
			}
			break
		}
	}
	var zero T
	return zero, false
}

// Remove a node from the the set.
func (s *SkipSet[T]) Remove(value T) bool {
	var (
		nodeToRemove *node[T]
		isMarked     bool // represents if this operation mark the node
		topLayer     = -1
		preds, succs [maxLevel]*node[T]
	)
	for {
		lFound := s.findNodeRemove(value, &preds, &succs)
		if isMarked || // this process mark this node or we can find this node in the skip list
			lFound != -1 && succs[lFound].flags.MGet(fullyLinked|marked, fullyLinked) && (int(succs[lFound].level)-1) == lFound {
			if !isMarked { // we don't mark this node for now
				nodeToRemove = succs[lFound]
				topLayer = lFound
				nodeToRemove.mu.Lock()
				if nodeToRemove.flags.Get(marked) {
					// The node is marked by another process,
					// the physical deletion will be accomplished by another process.
					nodeToRemove.mu.Unlock()
					return false
				}
				nodeToRemove.flags.SetTrue(marked)
				isMarked = true
			}
			// Accomplish the physical deletion.
			var (
				highestLocked        = -1 // the highest level being locked by this process
				valid                = true
				pred, succ, prevPred *node[T]
			)
			for layer := 0; valid && (layer <= topLayer); layer++ {
				pred, succ = preds[layer], succs[layer]
				if pred != prevPred { // the node in this layer could be locked by previous loop
					pred.mu.Lock()
					highestLocked = layer
					prevPred = pred
				}
				// valid check if there is another node has inserted into the skip list in this layer
				// during this process, or the previous is removed by another process.
				// It is valid if:
				// 1. the previous node exists.
				// 2. no another node has inserted into the skip list in this layer.
				valid = !pred.flags.Get(marked) && pred.loadNext(layer) == succ
			}
			if !valid {
				unlockNode(preds, highestLocked)
				continue
			}
			for i := topLayer; i >= 0; i-- {
				// Now we own the `nodeToRemove`, no other goroutine will modify it.
				// So we don't need `nodeToRemove.loadNext`
				preds[i].atomicStoreNext(i, nodeToRemove.loadNext(i))
			}
			nodeToRemove.mu.Unlock()
			unlockNode(preds, highestLocked)
			atomic.AddInt64(&s.length, -1)
			return true
		}
		return false
	}
}

// Range calls f sequentially for each value present in the  set.
// If f returns false, range stops the iteration.
func (s *SkipSet[T]) Range(f func(value T) bool) {
	x := s.header.atomicLoadNext(0)
	for x != nil {
		if !x.flags.MGet(fullyLinked|marked, fullyLinked) {
			x = x.atomicLoadNext(0)
			continue
		}
		if !f(x.value) {
			break
		}
		x = x.atomicLoadNext(0)
	}
}

// AscendGreaterEqual calls f sequentially for each value present in the the set greater than or equal to value.
// If f returns false, range stops the iteration.
func (s *SkipSet[T]) AscendGreaterEqual(value T, f func(value T) bool) {
	var preds, succs [maxLevel]*node[T]
	x := s.findNode(value, &preds, &succs)
	if x == nil && succs[0] != nil && s.less(value, succs[0].value) {
		x = succs[0]
	}
	for x != nil {
		if !x.flags.MGet(fullyLinked|marked, fullyLinked) {
			x = x.atomicLoadNext(0)
			continue
		}
		if !f(x.value) {
			break
		}
		x = x.atomicLoadNext(0)
	}
}

type ValueOrFuncValue[T any] interface {
	any | func() T
}

func getValue[T any](v ValueOrFuncValue[T]) T {
	switch any(v).(type) {
	case func() T:
		return v.(func() T)()
	default:
		return v.(T)
	}
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (s SkipSet[T]) LoadOrStore(value T, newValue ValueOrFuncValue[T]) (actual T, loaded bool) {
	level := s.randomlevel()
	var preds, succs [maxLevel]*node[T]
	for {
		lFound := s.findNodeAdd(value, &preds, &succs)
		if lFound != -1 { // indicating the value is already in the skip-list
			nodeFound := succs[lFound]
			if !nodeFound.flags.Get(marked) {
				for !nodeFound.flags.Get(fullyLinked) {
					// The node is not yet fully linked, just waits until it is.
					runtime.Gosched()
				}
				return nodeFound.value, true
			}
			// If the node is marked, represents some other thread is in the process of deleting this node,
			// we need to add this node in next loop.
			continue
		}

		// Store this node into skip list.
		var (
			highestLocked        = -1 // the highest level being locked by this process
			valid                = true
			pred, succ, prevPred *node[T]
		)
		for layer := 0; valid && layer < level; layer++ {
			pred = preds[layer]   // target node's previous node
			succ = succs[layer]   // target node's next node
			if pred != prevPred { // the node in this layer could be locked by previous loop
				pred.mu.Lock()
				highestLocked = layer
				prevPred = pred
			}
			// valid check if there is another node has inserted into the skip list in this layer during this process.
			// It is valid if:
			// 1. The previous node and next node both are not marked.
			// 2. The previous node's next node is succ in this layer.
			valid = !pred.flags.Get(marked) && (succ == nil || !succ.flags.Get(marked)) && pred.loadNext(layer) == succ
		}
		if !valid {
			unlockNode(preds, highestLocked)
			continue
		}

		// we are now at the safe-point, we can safely insert this node into the skip list.
		value = getValue(newValue)

		nn := newNode(value, level)
		for layer := 0; layer < level; layer++ {
			nn.storeNext(layer, succs[layer])
			preds[layer].atomicStoreNext(layer, nn)
		}
		nn.flags.SetTrue(fullyLinked)
		unlockNode(preds, highestLocked)
		atomic.AddInt64(&s.length, 1)
		return value, false
	}
}

// Len return the length of this the set.
func (s *SkipSet[T]) Len() int {
	return int(atomic.LoadInt64(&s.length))
}

type node[T any] struct {
	value T
	next  optionalArray // [level]*node
	mu    sync.Mutex
	flags bitflag
	level uint32
}

func newNode[T any](value T, level int) *node[T] {
	node := &node[T]{
		value: value,
		level: uint32(level),
	}
	if level > op1 {
		node.next.Extra = new([op2]unsafe.Pointer)
	}
	return node
}

func (n *node[T]) loadNext(i int) *node[T] {
	return (*node[T])(n.next.Load(i))
}

func (n *node[T]) storeNext(i int, node *node[T]) {
	n.next.Store(i, unsafe.Pointer(node))
}

func (n *node[T]) atomicLoadNext(i int) *node[T] {
	return (*node[T])(n.next.AtomicLoad(i))
}

func (n *node[T]) atomicStoreNext(i int, node *node[T]) {
	n.next.AtomicStore(i, unsafe.Pointer(node))
}

func test() {
	s := New(func(a, b int64) bool {
		return a < b
	})
	s.Len()
}
