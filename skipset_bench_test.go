package skipset

import (
	"math"
	"strconv"
	"sync"
	"testing"
)

const (
	initsize = 1 << 10 // for `contains` `1Remove9Add90Contains` `1Range9Remove90Add900Contains`
	randN    = math.MaxUint32
)

func BenchmarkInt64(b *testing.B) {
	all := []benchInt64Task{{
		name: "skipset", New: func() int64Set {
			return New(func(a, b int64) bool {
				return a < b
			})
		}}}
	all = append(all, benchInt64Task{
		name: "sync.Map", New: func() int64Set {
			return new(int64SyncMap)
		}})
	benchAdd(b, all)
	benchContains50Hits(b, all)
	bench30Add70Contains(b, all)
	bench1Remove9Add90Contains(b, all)
	bench1Range9Remove90Add900Contains(b, all)
}

func BenchmarkString(b *testing.B) {
	all := []benchStringTask{{
		name: "skipset", New: func() stringSet {
			return New(func(a, b string) bool {
				return a < b
			})
		}}}
	all = append(all, benchStringTask{
		name: "sync.Map", New: func() stringSet {
			return new(stringSyncMap)
		}})
	benchStringAdd(b, all)
	benchStringContains50Hits(b, all)
	benchString30Add70Contains(b, all)
	benchString1Remove9Add90Contains(b, all)
	benchString1Range9Remove90Add900Contains(b, all)
}

func benchAdd(b *testing.B, benchTasks []benchInt64Task) {
	for _, v := range benchTasks {
		b.Run("Store/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					s.Store(int64(fastrandUint32n(randN)))
				}
			})
		})
	}
}

func BenchmarkSet(b *testing.B) {
	s := New(func(a, b int) bool {
		return a < b
	})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Store(int(fastrandUint32n(randN)))
		}
	})
}

func BenchmarkStore(b *testing.B) {
	var s sync.Map
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Store(int(fastrandUint32n(randN)), 1)
		}
	})
}

func benchContains50Hits(b *testing.B, benchTasks []benchInt64Task) {
	for _, v := range benchTasks {
		b.Run("Contains50Hits/"+v.name, func(b *testing.B) {
			const rate = 2
			s := v.New()
			for i := 0; i < initsize*rate; i++ {
				if fastrandUint32n(rate) == 0 {
					s.Store(int64(i))
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = s.Contains(int64(fastrandUint32n(initsize * rate)))
				}
			})
		})
	}
}

func bench30Add70Contains(b *testing.B, benchTasks []benchInt64Task) {
	for _, v := range benchTasks {
		b.Run("30Add70Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(10)
					if u < 3 {
						s.Store(int64(fastrandUint32n(randN)))
					} else {
						s.Contains(int64(fastrandUint32n(randN)))
					}
				}
			})
		})
	}
}

func bench1Remove9Add90Contains(b *testing.B, benchTasks []benchInt64Task) {
	for _, v := range benchTasks {
		b.Run("1Remove9Add90Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(100)
					if u < 9 {
						s.Store(int64(fastrandUint32n(randN)))
					} else if u == 10 {
						s.Remove(int64(fastrandUint32n(randN)))
					} else {
						s.Contains(int64(fastrandUint32n(randN)))
					}
				}
			})
		})
	}
}

func bench1Range9Remove90Add900Contains(b *testing.B, benchTasks []benchInt64Task) {
	for _, v := range benchTasks {
		b.Run("1Range9Remove90Add900Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(1000)
					if u == 0 {
						s.Range(func(score int64) bool {
							return true
						})
					} else if u > 10 && u < 20 {
						s.Remove(int64(fastrandUint32n(randN)))
					} else if u >= 100 && u < 190 {
						s.Store(int64(fastrandUint32n(randN)))
					} else {
						s.Contains(int64(fastrandUint32n(randN)))
					}
				}
			})
		})
	}
}

func benchStringAdd(b *testing.B, benchTasks []benchStringTask) {
	for _, v := range benchTasks {
		b.Run("Store/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					s.Store(strconv.Itoa(int(fastrandUint32n(randN))))
				}
			})
		})
	}
}

func benchStringContains50Hits(b *testing.B, benchTasks []benchStringTask) {
	for _, v := range benchTasks {
		b.Run("Contains50Hits/"+v.name, func(b *testing.B) {
			const rate = 2
			s := v.New()
			for i := 0; i < initsize*rate; i++ {
				if fastrandUint32n(rate) == 0 {
					s.Store(strconv.Itoa(int(fastrandUint32n(randN))))
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = s.Contains(strconv.Itoa(int(fastrandUint32n(randN))))
				}
			})
		})
	}
}

func benchString30Add70Contains(b *testing.B, benchTasks []benchStringTask) {
	for _, v := range benchTasks {
		b.Run("30Add70Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(10)
					if u < 3 {
						s.Store(strconv.Itoa(int(fastrandUint32n(randN))))
					} else {
						s.Contains(strconv.Itoa(int(fastrandUint32n(randN))))
					}
				}
			})
		})
	}
}

func benchString1Remove9Add90Contains(b *testing.B, benchTasks []benchStringTask) {
	for _, v := range benchTasks {
		b.Run("1Remove9Add90Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(100)
					if u < 9 {
						s.Store(strconv.Itoa(int(fastrandUint32n(randN))))
					} else if u == 10 {
						s.Remove(strconv.Itoa(int(fastrandUint32n(randN))))
					} else {
						s.Contains(strconv.Itoa(int(fastrandUint32n(randN))))
					}
				}
			})
		})
	}
}

func benchString1Range9Remove90Add900Contains(b *testing.B, benchTasks []benchStringTask) {
	for _, v := range benchTasks {
		b.Run("1Range9Remove90Add900Contains/"+v.name, func(b *testing.B) {
			s := v.New()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u := fastrandUint32n(1000)
					if u == 0 {
						s.Range(func(score string) bool {
							return true
						})
					} else if u > 10 && u < 20 {
						s.Remove(strconv.Itoa(int(fastrandUint32n(randN))))
					} else if u >= 100 && u < 190 {
						s.Store(strconv.Itoa(int(fastrandUint32n(randN))))
					} else {
						s.Contains(strconv.Itoa(int(fastrandUint32n(randN))))
					}
				}
			})
		})
	}
}

type benchInt64Task struct {
	name string
	New  func() int64Set
}

type int64Set interface {
	Store(x int64) bool
	Contains(x int64) bool
	Remove(x int64) bool
	Range(f func(value int64) bool)
}

type int64SyncMap struct {
	data sync.Map
}

func (m *int64SyncMap) Store(x int64) bool {
	m.data.Store(x, struct{}{})
	return true
}

func (m *int64SyncMap) Contains(x int64) bool {
	_, ok := m.data.Load(x)
	return ok
}

func (m *int64SyncMap) Remove(x int64) bool {
	m.data.Delete(x)
	return true
}

func (m *int64SyncMap) Range(f func(value int64) bool) {
	m.data.Range(func(key, _ interface{}) bool {
		return !f(key.(int64))
	})
}

type benchStringTask struct {
	name string
	New  func() stringSet
}

type stringSet interface {
	Store(x string) bool
	Contains(x string) bool
	Remove(x string) bool
	Range(f func(value string) bool)
}

type stringSyncMap struct {
	data sync.Map
}

func (m *stringSyncMap) Store(x string) bool {
	m.data.Store(x, struct{}{})
	return true
}

func (m *stringSyncMap) Contains(x string) bool {
	_, ok := m.data.Load(x)
	return ok
}

func (m *stringSyncMap) Remove(x string) bool {
	m.data.Delete(x)
	return true
}

func (m *stringSyncMap) Range(f func(value string) bool) {
	m.data.Range(func(key, _ interface{}) bool {
		return !f(key.(string))
	})
}
