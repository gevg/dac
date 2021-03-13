package dac

import (
	"math"
	"math/rand"
	"testing"
)

func TestIterator(t *testing.T) {
	const n = 10
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, ^uint64(0))

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)

	it := NewIterator(d)
	for i := 0; i < n; i++ {
		v, err := it.Value(i)
		if err != nil {
			t.Errorf("err Value(%d): %s\n", i, err)
		}
		if v != numbers[i] {
			t.Errorf("Value(%d) - got: %d, want: %d\n", i, v, numbers[i])
		}
		for {
			k, got, ok := it.Next()
			if !ok {
				break
			}
			want := numbers[k]
			if got != want {
				t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
			}
		}
	}
}

func BenchmarkIteratorValue(b *testing.B) { // 13.1 ns/op    0 B/op    0 allocs/op    l:  1_000
	//                                         53.1 ns/op    0 B/op    0 allocs/op    l: 10_000
	const n = 1_000 //                         11.7 ns/op    0 B/op    0 allocs/op    l:  1_000
	//                                         20.3 ns/op    0 B/op    0 allocs/op    l: 10_000
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		d.WriteUint64(zipf.Uint64())
	}
	d.Close()

	it := NewIterator(d)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			it.Value(j)
		}
	}
}

func BenchmarkIteratorNext(b *testing.B) { // 3.57 ns/op    0 B/op    0 allocs/op    l:  1_000
	//                                        3.88 ns/op    0 B/op    0 allocs/op    l: 10_000
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		d.WriteUint64(zipf.Uint64())
	}

	it := NewIterator(d)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			it.Next()
		}
		it.Reset()
	}
}
