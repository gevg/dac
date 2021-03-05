package dac

import (
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

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for i := range numbers {
		d.WriteUint64(numbers[i])
	}

	it := NewIterator(d)
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

func BenchmarkIterator(b *testing.B) { // 3.87 ns/op    0 B/op    0 allocs/op    l:  1_000
	//                                    3.88 ns/op    0 B/op    0 allocs/op    l: 10_000
	const n = 10_000
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, ^uint64(0))

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := range numbers {
		d.WriteUint64(numbers[i])
	}

	it := NewIterator(d)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			_, _, _ = it.Next()
		}
	}
}
