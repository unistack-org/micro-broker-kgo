package kgo

import (
	"fmt"
	"math/rand"
	"testing"
)

// Point benchmarks for map operations
// These benchmarks test individual operations with consumers map
// Works with both mutex and sync.Map implementations via helper methods

func Benchmark_MapRead(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()
			keys := make([]tp, 0, size)
			for i := 0; i < size; i++ {
				key := tp{"topic", int32(i)}
				s.setConsumer(key, &consumer{})
				keys = append(keys, key)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					key := keys[i%len(keys)]
					_ = s.getConsumer(key)
					i++
				}
			})
		})
	}
}

func Benchmark_MapWrite(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()
			for i := 0; i < size; i++ {
				s.setConsumer(tp{"topic", int32(i)}, &consumer{})
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int32(0)
				c := &consumer{}
				for pb.Next() {
					key := tp{"topic", i % int32(size)}
					s.setConsumer(key, c)
					i++
				}
			})
		})
	}
}

func Benchmark_MapDelete(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < size; j++ {
					s.setConsumer(tp{"topic", int32(j)}, &consumer{})
				}
				b.StartTimer()

				for j := 0; j < size; j++ {
					key := tp{"topic", int32(j)}
					s.deleteConsumer(key)
				}
			}
		})
	}
}

func Benchmark_MapCopy(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()
			for i := 0; i < size; i++ {
				s.setConsumer(tp{"topic", int32(i)}, &consumer{})
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tpc := s.copyConsumers()
					_ = tpc
				}
			})
		})
	}
}

func Benchmark_MapMixed(b *testing.B) {
	sizes := []int{10, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()
			keys := make([]tp, 0, size)
			for i := 0; i < size; i++ {
				key := tp{"topic", int32(i)}
				s.setConsumer(key, &consumer{})
				keys = append(keys, key)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				i := 0
				c := &consumer{}
				for pb.Next() {
					op := rng.Intn(100)
					switch {
					case op < 80: // 80% read
						key := keys[i%len(keys)]
						_ = s.getConsumer(key)
					case op < 90: // 10% write
						key := tp{"topic", int32(i % size)}
						s.setConsumer(key, c)
					default: // 10% copy
						tpc := s.copyConsumers()
						_ = tpc
					}
					i++
				}
			})
		})
	}
}

// Parallel contention benchmarks
func Benchmark_MapContention(b *testing.B) {
	goroutines := []int{1, 4, 8, 16, 32}
	size := 100

	for _, numG := range goroutines {
		b.Run(fmt.Sprintf("goroutines=%d", numG), func(b *testing.B) {
			s := &Subscriber{}
			s.initConsumers()
			keys := make([]tp, 0, size)
			for i := 0; i < size; i++ {
				key := tp{"topic", int32(i)}
				s.setConsumer(key, &consumer{})
				keys = append(keys, key)
			}

			b.SetParallelism(numG)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				i := 0
				c := &consumer{}
				for pb.Next() {
					op := rng.Intn(100)
					switch {
					case op < 80:
						key := keys[i%len(keys)]
						_ = s.getConsumer(key)
					case op < 90:
						key := tp{"topic", int32(i % size)}
						s.setConsumer(key, c)
					default:
						tpc := s.copyConsumers()
						_ = tpc
					}
					i++
				}
			})
		})
	}
}