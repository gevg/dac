package dac

import (
	"math"
	"math/rand"
	"testing"
)

func TestRWUint64(t *testing.T) {
	const n = 100
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.17, 1.06, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	k := 0
	for _, want := range numbers {
		d.WriteUint64(want)
		d.WriteUint643(want)

		k++
		got, err := d.ReadUint64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}

		k++
	}
}

func TestWriteList(t *testing.T) {
	const n = 1_000
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.17, 1.06, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, _ := New(n)
	d.WriteList(numbers)

	for k, want := range numbers {
		got, err := d.ReadUint64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestFrom(t *testing.T) {
	const n = 100
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)

	for k, want := range numbers {
		got, err := d.ReadUint64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestReadList(t *testing.T) {
	const n = 100
	numbers := make([]uint64, n)
	values := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, _ := New(n)

	d.WriteList(numbers)
	d.ReadList(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadWriteBool(t *testing.T) {
	const n = 100

	numbers := make([]bool, n)

	rand.Seed(15)
	for i := range numbers {
		if a := rand.Int31n(2); a == 1 {
			numbers[i] = true
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteBool(v)
	}

	for k, want := range numbers {
		got, err := d.ReadBool(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteUint8(t *testing.T) {
	const n = 100

	numbers := make([]uint8, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = uint8(rand.Int31n(math.MaxUint8))
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteUint8(v)
	}

	for k, want := range numbers {
		got, err := d.ReadUint8(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteUint16(t *testing.T) {
	const n = 100

	numbers := make([]uint16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	for i := range numbers {
		numbers[i] = uint16(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteUint16(v)
	}

	for k, want := range numbers {
		got, err := d.ReadUint16(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteUint32(t *testing.T) {
	const n = 100

	numbers := make([]uint32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	for i := range numbers {
		numbers[i] = uint32(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteUint32(v)
	}

	for k, want := range numbers {
		got, err := d.ReadUint32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteUint64(t *testing.T) {
	const n = 100

	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteUint64(v)
	}

	for k, want := range numbers {
		got, err := d.ReadUint64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt8(t *testing.T) {
	const n = 100

	numbers := make([]int8, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int8(rand.Int31n(math.MaxUint8+1) + math.MinInt8)
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteInt8(v)
	}

	for k, want := range numbers {
		got, err := d.ReadInt8(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt16(t *testing.T) {
	const n = 100

	numbers := make([]int16, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int16(rand.Int31n(math.MaxUint16+1) + math.MinInt16)
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteInt16(v)
	}

	for k, want := range numbers {
		got, err := d.ReadInt16(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt32(t *testing.T) {
	const n = 100

	numbers := make([]int32, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int32(rand.Int63n(math.MaxUint32+1) + math.MinInt32)
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteInt32(v)
	}

	for k, want := range numbers {
		got, err := d.ReadInt32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt64(t *testing.T) {
	const n = 100

	numbers := make([]int64, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Int63n(math.MaxInt64) + math.MinInt64
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteInt64(v)
	}

	for k, want := range numbers {
		got, err := d.ReadInt64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteFloat32(t *testing.T) {
	const n = 100

	numbers := make([]float32, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Float32()
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteFloat32(v)
	}

	for k, want := range numbers {
		got, err := d.ReadFloat32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteFloat64(t *testing.T) {
	const n = 100

	numbers := make([]float64, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Float64()
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteFloat64(v)
	}

	for k, want := range numbers {
		got, err := d.ReadFloat64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func BenchmarkWriteBool(b *testing.B) { // 10.5 ns/op   0 B/op   0 allocs/op
	const n = 1000

	numbers := make([]bool, n)

	rand.Seed(15)
	for i := range numbers {
		if a := rand.Int31n(2); a == 1 {
			numbers[i] = true
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteBool(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint8(b *testing.B) { // 5.91 ns/op   0 B/op   0 allocs/op
	const n = 1000

	numbers := make([]uint8, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	for i := range numbers {
		numbers[i] = uint8(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteUint8(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint16(b *testing.B) { // 7.60 ns/op   0 B/op   0 allocs/op
	const n = 1000

	numbers := make([]uint16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	for i := range numbers {
		numbers[i] = uint16(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteUint16(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint32(b *testing.B) { // 8.69 ns/op   0 B/op   0 allocs/op
	const n = 1000

	numbers := make([]uint32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	for i := range numbers {
		numbers[i] = uint32(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteUint32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint64(b *testing.B) { // 8.79 ns/op   0 B/op   0 allocs/op
	const n = 1000

	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteUint64(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint64Unsafe(b *testing.B) { // 8.77 ns/op    0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteUint643(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteList(b *testing.B) { // 5.63 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteList(numbers)
		d.Reset()
	}
}

func BenchmarkReadList(b *testing.B) { // 4.05 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}
	d.WriteList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadList(numbers)
	}
}

func BenchmarkWriteInt8(b *testing.B) { // 8.86 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int8, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int8(rand.Intn(math.MaxUint8+1) + math.MinInt8)

	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteInt8(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteInt16(b *testing.B) { // 9.54 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int16, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int16(rand.Intn(math.MaxUint16+1) + math.MinInt16)

	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteInt16(v)
		}
		d.Reset()
	}
}

// // TODOs
// // Moet ik ook het int datatype ondersteunen???Misschien wel!
// // BenchmarkWrite is afhankelijk van de lengte. Testen op verschillende lengtes, maar pas wanneer rank volledig is geimplementeerd!!!

func BenchmarkWriteInt32(b *testing.B) { // 16.1 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int32, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int32(rand.Intn(math.MaxUint32+1) + math.MinInt32)
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteInt32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteInt64(b *testing.B) { // 29.2 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int64, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = int64(rand.Intn(math.MaxInt64) + math.MinInt64)
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteInt64(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteFloat32(b *testing.B) { // 15.6 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]float32, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Float32() - 1
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteFloat32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteFloat64(b *testing.B) { // 28.3 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]float64, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.NormFloat64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.WriteFloat64(v)
		}
		d.Reset()
	}
}

func BenchmarkReadUint8(b *testing.B) { // 4.59 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := range numbers {
			d.ReadUint8(k)
		}
	}
}

func BenchmarkReadUint16(b *testing.B) { // 8.71 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := range numbers {
			d.ReadUint16(k)
		}
	}
}

func BenchmarkReadUint32(b *testing.B) { // 12.4 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := range numbers {
			d.ReadUint32(k)
		}
	}
}

func BenchmarkReadUint64(b *testing.B) { // 13.6 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := range numbers {
			d.ReadUint64(k)
		}
	}
}

// func BenchmarkReadInt64(b *testing.B) { // 13.3 ns/op   0 B/op   0 allocs/op
// 	d := New(0)

// 	rand.Seed(15)
// 	for i := 0; i < 1000; i++ {
// 		d.WriteInt64(rand.Int63())
// 	}

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	var k int64
// 	for i := 0; i < b.N; i++ {
// 		for j := uint32(0); j < 1000; j++ {
// 			k = d.ReadInt64(j)
// 		}
// 	}
// 	_ = k
// }

// func BenchmarkReadFloat32(b *testing.B) { // 7.98 ns/op   0 B/op   0 allocs/op bij 1e3
// 	d := New(0) //                            6.63 ns/op   0 B/op   0 allocs/op bij 1e6
// 	//                                        80.4 ns/op   0 B/op   0 allocs/op bij 1e3
// 	rand.Seed(15)
// 	for i := 0; i < 1000; i++ {
// 		d.WriteFloat32(rand.Float32())
// 	}

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	var k float32
// 	for i := 0; i < b.N; i++ {
// 		for j := uint32(0); j < 1000; j++ { // Ik vrees dat hier MLP aan het werk is. En alles is strided, niet random!
// 			k = d.ReadFloat32(j)
// 		}
// 	}
// 	_ = k
// }

// func BenchmarkReadFloat64(b *testing.B) { // 10.5 ns/op   0 B/op   0 allocs/op bij 1e3 strided
// 	d := New() //                             6.8 ns/op   0 B/op   0 allocs/op bij 1e6 strided
// 	//                                         164 ns/op   0 B/op   0 allocs/op bij 1e3 strided
// 	rand.Seed(15)
// 	for i := 0; i < 1_000; i++ {
// 		d.WriteFloat64(rand.Float64())
// 	}

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	var k float64
// 	for i := 0; i < b.N; i++ {
// 		for j := uint32(0); j < 1_000; j++ {
// 			k = d.ReadFloat64(j)
// 		}
// 	}
// 	_ = k
// }
