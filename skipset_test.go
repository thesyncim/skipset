package skipset

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func Example() {
	l := New(func(a, b int) bool {
		return a < b
	})

	for _, v := range []int{10, 12, 15} {
		if l.Store(v) {
			fmt.Println("skipset add", v)
		}
	}

	if l.Contains(10) {
		fmt.Println("skipset contains 10")
	}

	l.Range(func(value int) bool {
		fmt.Println("skipset range found ", value)
		return true
	})

	l.Remove(15)
	fmt.Printf("skipset contains %d items\r\n", l.Len())
}

func TestIntSet(t *testing.T) {
	// Correctness.
	l := New(func(a, b int) bool {
		return a < b
	})
	if l.length != 0 {
		t.Fatal("invalid length")
	}
	if l.Contains(0) {
		t.Fatal("invalid contains")
	}

	if !l.Store(0) || l.length != 1 {
		t.Fatal("invalid add")
	}
	if !l.Contains(0) {
		t.Fatal("invalid contains")
	}
	if !l.Remove(0) || l.length != 0 {
		t.Fatal("invalid remove")
	}

	if !l.Store(20) || l.length != 1 {
		t.Fatal("invalid add")
	}
	if !l.Store(22) || l.length != 2 {
		t.Fatal("invalid add")
	}
	if !l.Store(21) || l.length != 3 {
		t.Fatal("invalid add")
	}

	var i int
	l.Range(func(score int) bool {
		if i == 0 && score != 20 {
			t.Fatal("invalid range")
		}
		if i == 1 && score != 21 {
			t.Fatal("invalid range")
		}
		if i == 2 && score != 22 {
			t.Fatal("invalid range")
		}
		i++
		return true
	})

	if !l.Remove(21) || l.length != 2 {
		t.Fatal("invalid remove")
	}

	i = 0
	l.Range(func(score int) bool {
		if i == 0 && score != 20 {
			t.Fatal("invalid range")
		}
		if i == 1 && score != 22 {
			t.Fatal("invalid range")
		}
		i++
		return true
	})

	const num = math.MaxInt16
	// Make rand shuffle array.
	// The testArray contains [1,num]
	testArray := make([]int, num)
	testArray[0] = num + 1
	for i := 1; i < num; i++ {
		// We left 0, because it is the default score for head and tail.
		// If we check the skipset contains 0, there must be something wrong.
		testArray[i] = int(i)
	}
	for i := len(testArray) - 1; i > 0; i-- { // Fisher–Yates shuffle
		j := fastrandUint32n(uint32(i + 1))
		testArray[i], testArray[j] = testArray[j], testArray[i]
	}

	// Concurrent add.
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		i := i
		wg.Add(1)
		go func() {
			l.Store(testArray[i])
			wg.Done()
		}()
	}
	wg.Wait()
	if l.length != int64(num) {
		t.Fatalf("invalid length expected %d, got %d", num, l.length)
	}

	// Don't contains 0 after concurrent addion.
	if l.Contains(0) {
		t.Fatal("contains 0 after concurrent addion")
	}

	// Concurrent contains.
	for i := 0; i < num; i++ {
		i := i
		wg.Add(1)
		go func() {
			if !l.Contains(testArray[i]) {
				wg.Done()
				panic(fmt.Sprintf("add doesn't contains %d", i))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// Concurrent remove.
	for i := 0; i < num; i++ {
		i := i
		wg.Add(1)
		go func() {
			if !l.Remove(testArray[i]) {
				wg.Done()
				panic(fmt.Sprintf("can't remove %d", i))
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if l.length != 0 {
		t.Fatalf("invalid length expected %d, got %d", 0, l.length)
	}

	// Test all methods.
	const smallRndN = 1 << 8
	for i := 0; i < 1<<16; i++ {
		wg.Add(1)
		go func() {
			r := fastrandUint32n(num)
			if r < 333 {
				l.Store(int(fastrandUint32n(smallRndN)) + 1)
			} else if r < 666 {
				l.Contains(int(fastrandUint32n(smallRndN)) + 1)
			} else if r != 999 {
				l.Remove(int(fastrandUint32n(smallRndN)) + 1)
			} else {
				var pre int
				l.Range(func(score int) bool {
					if score <= pre { // 0 is the default value for header and tail score
						panic("invalid content")
					}
					pre = score
					return true
				})
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// Correctness 2.
	var (
		x = New(func(a, b int) bool {
			return a < b
		})
		y = New(func(a, b int) bool {
			return a < b
		})
		count = 10000
	)

	for i := 0; i < count; i++ {
		x.Store(i)
	}

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			x.Range(func(score int) bool {
				if x.Remove(score) {
					if !y.Store(score) {
						panic("invalid add")
					}
				}
				return true
			})
			wg.Done()
		}()
	}
	wg.Wait()
	if x.Len() != 0 || y.Len() != count {
		t.Fatal("invalid length")
	}

	// Concurrent Store and Remove in small zone.
	x = New(func(a, b int) bool {
		return a < b
	})
	var (
		addcount    uint64 = 0
		removecount uint64 = 0
	)

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				if fastrandUint32n(2) == 0 {
					if x.Remove(int(fastrandUint32n(10))) {
						atomic.AddUint64(&removecount, 1)
					}
				} else {
					if x.Store(int(fastrandUint32n(10))) {
						atomic.AddUint64(&addcount, 1)
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if addcount < removecount {
		panic("invalid count")
	}
	if addcount-removecount != uint64(x.Len()) {
		panic("invalid count")
	}

	pre := -1
	x.Range(func(score int) bool {
		if score <= pre {
			panic("invalid content")
		}
		pre = score
		return true
	})

	// Correctness 3.
	s1 := New(func(a, b uint64) bool {
		return a < b
	})
	var s2 sync.Map
	var counter uint64
	for i := 0; i <= 10000; i++ {
		wg.Add(1)
		go func() {
			if fastrandUint32n(2) == 0 {
				r := fastrandUint32()
				s1.Store(uint64(r))
				s2.Store(uint64(r), nil)
			} else {
				r := atomic.AddUint64(&counter, 1)
				s1.Store(uint64(r))
				s2.Store(uint64(r), nil)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	s1.Range(func(value uint64) bool {
		_, ok := s2.Load(value)
		if !ok {
			t.Fatal(value)
		}
		return true
	})
	s2.Range(func(key, value interface{}) bool {
		k := key.(uint64)
		if !s1.Contains(k) {
			t.Fatal(value)
		}
		return true
	})
}

func TestIntSetDesc(t *testing.T) {
	s := New(func(a, b int) bool {
		return a > b
	})
	nums := []int{-1, 0, 5, 12}
	for _, v := range nums {
		s.Store(v)
	}
	i := len(nums) - 1
	s.Range(func(value int) bool {
		if nums[i] != value {
			t.Fatal("error")
		}
		i--
		return true
	})
}

func TestStringSet(t *testing.T) {
	x := New(func(a, b string) bool {
		return a < b
	})
	if !x.Store("111") || x.Len() != 1 {
		t.Fatal("invalid")
	}
	if !x.Store("222") || x.Len() != 2 {
		t.Fatal("invalid")
	}
	if x.Store("111") || x.Len() != 2 {
		t.Fatal("invalid")
	}
	if !x.Contains("111") || !x.Contains("222") {
		t.Fatal("invalid")
	}
	if !x.Remove("111") || x.Len() != 1 {
		t.Fatal("invalid")
	}
	if !x.Remove("222") || x.Len() != 0 {
		t.Fatal("invalid")
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			if !x.Store(strconv.Itoa(i)) {
				panic("invalid")
			}
			wg.Done()
		}()
	}
	wg.Wait()

	tmp := make([]int, 0, 100)
	x.Range(func(val string) bool {
		res, _ := strconv.Atoi(val)
		tmp = append(tmp, res)
		return true
	})
	sort.Ints(tmp)
	for i := 0; i < 100; i++ {
		if i != tmp[i] {
			t.Fatal("invalid")
		}
	}
}

func TestAscendGreaterEqual(t *testing.T) {
	const mapSize = 1 << 10

	m := New(func(a, b int64) bool {
		return a < b
	})
	want := []int64{}
	for n := int64(1); n <= mapSize; n += 2 {
		if n >= 100 {
			want = append(want, n)
		}
		m.Store(n)
	}

	i := 0
	m.AscendGreaterEqual(100, func(key int64) bool {
		if want[i] != key {
			t.Fatalf("expecting key %d, got %d", want[i], key)
		}
		i++
		return true
	})

	if len(want) != i {
		t.Fatalf("expecting ranged entries %d, got %d", len(want), i)
	}

	m.AscendGreaterEqual(int64(mapSize+1), func(key int64) bool {
		t.Fatalf("unexpected call")
		return true
	})
}

func TestSkipSet_LoadOrStore(t *testing.T) {
	type typ struct {
		Key, value string
	}

	s := New(func(a, b typ) bool {
		return a.Key < b.Key
	})

	v, loaded := s.LoadOrStore(typ{Key: "a"}, func() typ { return typ{Key: "a", value: "b"} })
	if loaded {
		t.Fatal("should not be loaded")
	}
	if s.Len() != 1 {
		t.Fatalf("should be 1, got %d", s.Len())
	}
	if v != (typ{Key: "a", value: "b"}) {
		t.Fatal("should be equal")
	}

	v, loaded = s.LoadOrStore(typ{Key: "a"}, func() typ { return typ{Key: "a", value: "b"} })
	if !loaded {
		t.Fatal("should be loaded")
	}
	if s.Len() != 1 {
		t.Fatalf("should be 1, got %d", s.Len())
	}
	if v != (typ{Key: "a", value: "b"}) {
		t.Fatal("should be equal")
	}

	v, loaded = s.LoadOrStore(typ{Key: "a"}, typ{Key: "a", value: "b"})
	if !loaded {
		t.Fatal("should be loaded")
	}
	if s.Len() != 1 {
		t.Fatalf("should be 1, got %d", s.Len())
	}
	if v != (typ{Key: "a", value: "b"}) {
		t.Fatal("should be equal")
	}
}

func TestStoreSet(t *testing.T) {
	type typ struct {
		Key   int
		value string
	}

	s := New(func(a, b typ) bool {
		return a.Key < b.Key
	})

	for i := 0; i < 100; i++ {
		s.Set(typ{Key: i, value: "initial"})

		i := i
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			s.Set(typ{Key: i, value: "updated"})

			item, found := s.Load(typ{Key: i})
			if found && item.value != "updated" {
				t.Fatalf("item should be updated")
			}
		})
	}
}

func TestMin(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		if s.Min() != 0 {
			t.Fatal("invalid")
		}
	})
	t.Run("monotonically increasing", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := 0; i < 10; i++ {
			s.Store(i)
			mn := s.Min()
			if mn != 0 {
				t.Fatalf("invalid: expected=%d, got=%d", 0, mn)
			}
		}
	})
	t.Run("monotonically decreasing", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := -1; i >= -10; i-- {
			s.Store(i)
			mn := s.Min()
			if mn != i {
				t.Fatalf("invalid: expected=%d, got=%d", i, mn)
			}
		}
	})
	t.Run("concurrent with stores - non-increasing min", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		wg.Add(1)
		s.Set(1001)
		go func() {
			defer wg.Done()
			for i := 1000; i >= 0; i-- {
				s.Set(i)
			}
		}()
		prevMin := 1001
		for i := 0; i < 1000; i++ {
			min := s.Max()
			if min > prevMin {
				t.Fatalf("max is expected to be monotonic, got %d after %d", min, prevMin)
			}
			prevMin = min
		}
		wg.Wait()
		expectOrder(t, s)
	})
	t.Run("concurrent with stores - min first and stores", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		s.Set(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= 100; i++ {
				s.Set(i)
			}
		}()
		for i := 2; i < 1000; i++ {
			min := s.Min()
			if min != 1 {
				t.Fatalf("min is expected to be 0, got %d", min)
			}
		}
		wg.Wait()
		expectOrder(t, s)
	})
	t.Run("concurrent with removes - min first removed", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := 0; i <= 1000; i++ {
			s.Set(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i <= 1000; i++ {
				s.Remove(i)
			}
		}()
		prevMin := 0
		for i := 0; i <= 1000; i++ {
			min := s.Min()
			if min < prevMin {
				// monotonic because we are removing items in order
				t.Fatalf("min is expected to be monotonic, got %d after %d", min, prevMin)
			}
		}
		expectOrder(t, s)
	})
}

func TestMax(t *testing.T) {
	t.Run("empty set", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		mx := s.Max()
		if mx != 0 {
			t.Fatalf("invalid: expected=%d, got=%d", 0, mx)
		}
	})
	t.Run("monotonically increasing", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := 0; i < 10; i++ {
			s.Store(i)
			mx := s.Max()
			if mx != i {
				t.Fatalf("invalid: expected=%d, got=%d", i, mx)
			}
		}
		expectOrder(t, s)
	})
	t.Run("monotonically decreasing", func(t *testing.T) {
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := 10; i >= 0; i-- {
			s.Store(i)
			mx := s.Max()
			if mx != 10 {
				t.Fatalf("invalid: expected=%d, got=%d", 10, mx)
			}
		}
		expectOrder(t, s)
	})
	t.Run("concurrent with stores - non-decreasing max", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				s.Set(i)
			}
		}()
		prevMax := 0
		for i := 0; i < 1000; i++ {
			max := s.Max()
			if max < prevMax {
				t.Fatalf("max is expected to be monotonic, got %d after %d", max, prevMax)
			}
			prevMax = max
		}
		wg.Wait()
		expectOrder(t, s)
	})
	t.Run("concurrent with max first and stores", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		s.Set(1000)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 999; i >= 0; i-- {
				s.Set(i)
			}
		}()
		for i := 0; i < 10_000; i++ {
			max := s.Max()
			if max != 1000 {
				t.Fatalf("max is expected to be 1000, got %d", max)
			}
		}
		wg.Wait()
		expectOrder(t, s)
	})
	t.Run("concurrent with removes - max first removed", func(t *testing.T) {
		var wg sync.WaitGroup
		s := New(func(a, b int) bool {
			return a < b
		})
		for i := 0; i <= 1000; i++ {
			s.Set(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1000; i >= 0; i-- {
				s.Remove(i)
			}
		}()
		prevMax := 1000
		for i := 0; i <= 1000; i++ {
			max := s.Max()
			if max > prevMax {
				// monotonic because we are removing items in order
				t.Fatalf("max is expected to be monotonic, got %d after %d", max, prevMax)
			}
		}
		expectOrder(t, s)
	})
}

func expectOrder(t *testing.T, s *SkipSet[int]) {
	t.Helper()
	if s.Len() == 0 {
		return
	}
	var prev int
	index := 0
	min, max := s.Min(), s.Max()
	s.Range(func(v int) bool {
		if index == 0 && v != min {
			t.Fatalf("invalid (min should be first): expected=%d, got=%d", min, v)
		}
		if index > 0 && v < prev {
			t.Fatalf("invalid (monotonicity violation): expected=%d, got=%d", prev, v)
		}
		prev = v
		index++
		return true
	})
	if prev != max {
		t.Fatalf("invalid max (max should be last): expected=%d, got=%d", max, prev)
	}
}
