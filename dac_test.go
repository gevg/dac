package dac

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

var inputBool = []bool{true, false, true, false, true, false, true, false, true, false}
var inputUint8 = []uint8{0, 1, 15, 16, 63, 64, 99, 100, 126, math.MaxUint8}
var inputUint16 = []uint16{0, 1, 15, 16, 63, 64, 99, 100, 126, math.MaxUint16}
var inputUint32 = []uint32{0, 1, 15, 16, 63, 64, 99, 100, 126, math.MaxUint32}
var inputUint64 = []uint64{0, 1, 127, 128, 129, 255, 256, 257, 80000, math.MaxUint64}
var inputInt8 = []int8{0, 1, -15, 16, -63, 64, -99, 100, math.MinInt8, math.MaxInt8}
var inputInt16 = []int16{0, -1, 127, -128, 129, -255, 256, -257, math.MinInt16, math.MaxInt16}
var inputInt32 = []int32{0, -1, 127, -128, 129, -255, 256, -257, math.MinInt32, math.MaxInt32}
var inputInt64 = []int64{0, -1, 127, -128, 129, -255, 256, -257, math.MinInt64, math.MaxInt64}
var inputFloat32 = []float32{0, 1.1, -15.12, 16.123, -63.1234, 64e3, -99e4, 100.12345e15, math.SmallestNonzeroFloat32, math.MaxFloat32}
var inputFloat64 = []float64{0, 1.1, -15.12, 16.123, -63.1234, 64e3, -99e4, 100.12345e15, math.SmallestNonzeroFloat64, math.MaxFloat64}

var output = [][]byte{
	{128},         //     0
	{129},         //     1
	{255},         //   127
	{128, 0},      //   128
	{129, 0},      //   129
	{255, 0},      //   255
	{128, 1},      //   256
	{129, 1},      //   257
	{255, 6},      //  1023
	{128, 112, 3}, // 80000
}

// Je moet nog een TestWriteReadUint64() toevoegen over een reeks van random getallen.
// Dit verhoogt de kans dat je nog speciale gevallen ontdekt.

func TestRWUint64(t *testing.T) {
	numbers := make([]uint64, 100000)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Uint64()
	}

	d := New(0)
	for _, want := range numbers {
		if got := d.ReadUint64(d.WriteUint64(want)); got != want {
			t.Errorf("got: %d, want: %d\n", got, want)
		}
	}
}

func TestWriteBool(t *testing.T) {
	d := New(0)

	for _, x := range inputBool {
		d.WriteBool(x)
	}
}

func TestWriteUint8(t *testing.T) {
	d := New(0)

	for _, x := range inputUint8 {
		d.WriteUint8(x)
	}
	// Je doet hier geen enkele test!!!!!!
}

func TestWriteUint16(t *testing.T) {
	d := New(0)

	for _, x := range inputUint16 {
		d.WriteUint16(x)
	}
}

func TestWriteUint32(t *testing.T) {
	d := New(0)

	for _, x := range inputUint32 {
		d.WriteUint32(x)
	}
}

func TestWriteUint64t(t *testing.T) {
	numbers := make([]uint64, 1000)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Uint64()
	}

	d := New(0)
	for i, want := range numbers {
		if got := d.ReadUint64(d.WriteUint64(want)); got != want {
			t.Errorf("%d: got: %d, want: %d\n", i, got, want)
		}
	}
}

func TestWriteUint64(t *testing.T) {
	d := New(0)

	for _, x := range inputUint64 {
		d.WriteUint64(x)
	}
}

func TestWriteInt8(t *testing.T) {
	d := New(0)

	for _, x := range inputInt8 {
		d.WriteInt8(x)
	}
}

func TestWriteInt16(t *testing.T) {
	d := New(0)

	for _, x := range inputInt16 {
		d.WriteInt16(x)
	}
}

func TestWriteInt32(t *testing.T) {
	d := New(0)

	for _, x := range inputInt32 {
		d.WriteInt32(x)
	}
}

func TestWriteInt64(t *testing.T) {
	d := New(0)

	for _, x := range inputInt64 {
		d.WriteInt64(x)
	}
}

func TestWriteFloat32(t *testing.T) {
	d := New(0)

	for _, x := range inputFloat32 {
		d.WriteFloat32(x)
	}
}

func TestWriteFloat64(t *testing.T) {
	d := New(0)

	for _, x := range inputFloat64 {
		d.WriteFloat64(x)
	}
}

func TestReadBool(t *testing.T) {
	d := New(0)
	for _, x := range inputBool {
		d.WriteBool(x)
	}

	for id, v := range inputBool {
		x := d.ReadBool(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%v != %v=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadUint8(t *testing.T) {
	d := New(0)
	for _, x := range inputUint8 {
		d.WriteUint8(x)
	}

	for id, v := range inputUint8 {
		x := d.ReadUint8(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadUint16(t *testing.T) {
	d := New(0)
	for _, x := range inputUint16 {
		d.WriteUint16(x)
	}

	for id, v := range inputUint16 {
		x := d.ReadUint16(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadUint32(t *testing.T) {
	d := New(0)
	for _, x := range inputUint32 {
		d.WriteUint32(x)
	}

	for id, v := range inputUint32 {
		x := d.ReadUint32(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadUint64(t *testing.T) {
	d := New(0)
	for _, x := range inputUint64 {
		d.WriteUint64(x)
	}

	for id, v := range inputUint64 {
		x := d.ReadUint64(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadUint64r(t *testing.T) {
	d := New(0)
	for _, x := range inputUint64 {
		d.WriteUint64r(x)
	}

	for id, v := range inputUint64 {
		x := d.ReadUint64r(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadInt8(t *testing.T) {
	d := New(0)
	for _, x := range inputInt8 {
		d.WriteInt8(x)
	}

	for id, v := range inputInt8 {
		x := d.ReadInt8(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadInt16(t *testing.T) {
	d := New(0)
	for _, x := range inputInt16 {
		d.WriteInt16(x)
	}

	for id, v := range inputInt16 {
		x := d.ReadInt16(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadInt32(t *testing.T) {
	d := New(0)
	for _, x := range inputInt32 {
		d.WriteInt32(x)
	}

	for id, v := range inputInt32 {
		x := d.ReadInt32(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadInt64(t *testing.T) {
	d := New(0)
	for _, x := range inputInt64 {
		d.WriteInt64(x)
	}

	for id, v := range inputInt64 {
		x := d.ReadInt64(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%d != %d=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadFloat32(t *testing.T) {
	d := New(0)
	for _, x := range inputFloat32 {
		d.WriteFloat32(x)
	}

	for id, v := range inputFloat32 {
		x := d.ReadFloat32(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%f != %f=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestReadFloat64(t *testing.T) {
	d := New(0)
	for _, x := range inputFloat64 {
		d.WriteFloat64(x)
	}

	for id, v := range inputFloat64 {
		x := d.ReadFloat64(uint32(id))
		if x != v {
			t.Errorf("x[%d]=%f != %f=input[%d]", id, x, v, id)
		}
	}
	fmt.Println()
}

func TestLongRead(t *testing.T) {
	d := New(0)
	for x := uint8(0); x < 255; x++ {
		_ = x
		d.WriteUint64(800000000)
	}

	for id := uint32(0); id < 255; id++ {
		d.ReadUint64(id)
	}
}

// TODO: Deze testen verbeteren door random getallen te schrijven. De gemiddelde lengte is nl. belangrijk om de performantie te meten.

func BenchmarkWriteBoolx(b *testing.B) { // 9.19 ns/op   6.07 B/op   0 allocs/op
	numbers := make([]bool, 1000)

	rand.Seed(15)
	for i := range numbers {
		if a := rand.Int31n(2); a == 1 {
			numbers[i] = true
		} else {
			numbers[i] = false
		}
	}

	d := New(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			d.WriteBool(x)
		}
	}
}

func BenchmarkWriteBool(b *testing.B) { // 7.99 ns/op   6.3 B/op   0 allocs/op
	d := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range inputBool {
			d.WriteBool(x)
		}
	}
}

func BenchmarkWriteUint8x(b *testing.B) { // 13.9 ns/op   8.77 B/op   0 allocs/op
	numbers := make([]uint8, 1000)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = uint8(rand.Int31n(math.MaxUint8))
	}

	d := New(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			d.WriteUint8(x)
		}
	}
}

func BenchmarkWriteUint8(b *testing.B) { // 9.12 ns/op   6.4 B/op   0 allocs/op
	d := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range inputUint8 {
			d.WriteUint8(x)
		}
	}
}

func BenchmarkWriteUint16x(b *testing.B) { // 19.5 ns/op   17.7 B/op   0 allocs/op
	numbers := make([]uint16, 1000)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = uint16(rand.Int31n(math.MaxUint16))
	}

	d := New(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			d.WriteUint16(x)
		}
	}
}

func BenchmarkWriteUint32x(b *testing.B) { // 29.5 ns/op   31.7 B/op   0 allocs/op
	numbers := make([]uint32, 1000)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Uint32()
	}

	d := New(0)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			d.WriteUint32(x)
		}
	}
}

// Is dit 62.8 ns/op tgv cache misses???
// Ik vermoed dat de allocaties het grootste probleem is.
// Write doet een append. Dit zou niet tot cache problemen mogen leiden,
// noch in DAC, noch in de ranks!!! Als je weet hoe groot de dictionary
// moet zijn, kan je op zijn minst chunks en de rank voor level 0 alloceren!
func BenchmarkWriteUint64x(b *testing.B) { // 47.0 ns/op   21.7 B/op   0.128 allocs/op : New()
	numbers := make([]uint64, 1000) //        41.4 ns/op   12.6 B/op   0.032 allocs/op : New2()
	// 58.3 - 56.2                            32.0 ns/op   11.4 B/op   0.019 allocs/op : without extend
	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Uint64()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d := New(0)
		// d := New2(1000, 0)
		for _, x := range numbers {
			d.WriteUint64(x)
		}
	}
}

func BenchmarkWriteUint64Unsafe(b *testing.B) { // 30.5 ns/op   0 B/op   0 allocs/op Unsafe
	//                                             27.0 ns/op   0 B/op   0 allocs/op Unsafe zonder Extend
	const num = 1000 //                            30.7 ns/op   0 B/op   0 allocs/op Unsafe2
	//                                             26.7 ns/op   0 B/op   0 allocs/op Unsafe2 zonder Extend
	numbers := make([]uint64, num)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Uint64()
	}

	d := New2(num, 0) // parameter optional maken!!!

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			d.WriteUint64Unsafe(x)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint64(b *testing.B) { // 75.9 ns/op   17.6 B/op   2.1 allocs/op
	for i := 0; i < b.N; i++ {
		d := New(0)
		for _, x := range inputUint64 {
			d.WriteUint64(x)
		}
	}
}

func BenchmarkWriteInt8x(b *testing.B) { // 13.7 ns/op   8.62 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]int8, 1000)
	for i := range numbers {
		numbers[i] = int8(rand.Int31n(math.MaxInt8))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteInt8(n)
		}
	}
}

func BenchmarkWriteInt8(b *testing.B) { // 24.4 ns/op   4.8 B/op   5 allocs/op
	for i := 0; i < b.N; i++ {
		d := New(0)
		for _, x := range inputInt8 {
			d.WriteInt8(x)
		}
	}
}

func BenchmarkWriteInt16x(b *testing.B) { // 20.1 ns/op   17.4 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]int16, 1000)
	for i := range numbers {
		numbers[i] = int16(rand.Int31n(math.MaxInt16))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteInt16(n)
		}
	}
}

// TODOs
// Moet ik ook het int datatype ondersteunen???Misschien wel!
// BenchmarkWrite is afhankelijk van de lengte. Testen op verschillende lengtes, maar pas wanneer rank volledig is geimplementeerd!!!

func BenchmarkWriteInt32x(b *testing.B) { // 30.3 ns/op   32.6 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]int32, 1000)
	for i := range numbers {
		numbers[i] = rand.Int31()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteInt32(n)
		}
	}
}

func BenchmarkWriteInt64x(b *testing.B) { // 55.4 ns/op   61.6 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]int64, 1000)
	for i := range numbers {
		numbers[i] = rand.Int63()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteInt64(n)
		}
	}
}

func BenchmarkWriteInt64(b *testing.B) { // 20.9 ns/op   20.7 B/op   0 allocs/op
	d := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range inputInt64 {
			d.WriteInt64(int64(x))
		}
	}
}

func BenchmarkWriteFloat32x(b *testing.B) { // 30.5 ns/op   30.4 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]float32, 1000)
	for i := range numbers {
		numbers[i] = rand.Float32()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteFloat32(n)
		}
	}
}

func BenchmarkWriteFloat32(b *testing.B) { // 23.3 ns/op   27.3 B/op   0 allocs/op
	d := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range inputFloat32 {
			d.WriteFloat32(x)
		}
	}
}

func BenchmarkWriteFloat64x(b *testing.B) { // 68.7 ns/op   56.4 B/op   0 allocs/op
	d := New(0)
	rand.Seed(15)

	numbers := make([]float64, 1000)
	for i := range numbers {
		numbers[i] = rand.Float64()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, n := range numbers {
			d.WriteFloat64(n)
		}
	}
}

func BenchmarkWriteFloat64(b *testing.B) { // 45.8 ns/op   49.5 B/op   0 allocs/op
	d := New(0)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range inputFloat64 {
			d.WriteFloat64(x)
		}
	}
}

func BenchmarkReadUint8x(b *testing.B) { // 6.57 ns/op   0 B/op   0 allocs/op
	d := New(0) //                          18.4 ns/op   0 B/op   0 allocs/op ???

	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteUint8(uint8(rand.Int31n(math.MaxUint8)))
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k uint8
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ {
			k = d.ReadUint8(j)
		}
	}
	_ = k
}

func BenchmarkReadUint8(b *testing.B) { // 6.88 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputUint8 {
		d.WriteUint8(x)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputUint8 {
			d.ReadUint8(uint32(j))
		}
	}
}

func BenchmarkReadUint16x(b *testing.B) { // 7.33 ns/op   0 B/op   0 allocs/op
	d := New(0) //                           41.1 ns/op   0 B/op   0 allocs/op

	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteUint16(uint16(rand.Int31n(math.MaxUint16)))
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k uint16
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ {
			k = d.ReadUint16(j)
		}
	}
	_ = k
}

func BenchmarkReadUint16(b *testing.B) { // 8.82 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputUint16 {
		d.WriteUint16(x)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputUint16 {
			d.ReadUint16(uint32(j))
		}
	}
}

func BenchmarkReadUint32x(b *testing.B) { // 8.32 ns/op   0 B/op   0 allocs/op
	d := New(0) //                           80.3 ns/op   0 B/op   0 allocs/op

	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteUint32(rand.Uint32())
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k uint32
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ {
			k = d.ReadUint32(j)
		}
	}
	_ = k
}

func BenchmarkReadUint64x(b *testing.B) { // 10.8 ns/op   0 B/op   0 allocs/op for 1e3
	d := New(0) //                           6.57 ns/op   0 B/op   0 allocs/op for 1e6
	//                                        177 ns/op   0 B/op   0 allocs/op for 1e3
	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteUint64(rand.Uint64())
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k uint64
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ { // Dit is sequentieel. Wat is de kost wanneer random??? Wat als we echt latency meten???
			k = d.ReadUint64(j)
		}
	}
	_ = k
}

func BenchmarkReadUint64(b *testing.B) { // 18.6 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputUint64 {
		d.WriteUint64(x)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputUint64 {
			d.ReadUint64(uint32(j))
		}
	}
}

func BenchmarkReadInt64x(b *testing.B) { // 13.3 ns/op   0 B/op   0 allocs/op
	d := New(0) //                           185 ns/op   0 B/op   0 allocs/op

	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteInt64(rand.Int63())
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k int64
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ {
			k = d.ReadInt64(j)
		}
	}
	_ = k
}

func BenchmarkReadInt64(b *testing.B) { // 27.8 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputInt64 {
		d.WriteInt64(int64(x))
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputInt64 {
			d.ReadInt64(uint32(j))
		}
	}
}

// func BenchmarkReadUint64(b *testing.B) { // 18.1 ns/op   0 B/op   0 allocs/op
// 	d := New(0)
// 	for _, x := range inputUint64 {
// 		d.WriteUint64(x)
// 	}
// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		for j := range inputUint64 {
// 			d.ReadUint64(uint32(j))
// 		}
// 	}
// }

func BenchmarkReadFloat32x(b *testing.B) { // 7.98 ns/op   0 B/op   0 allocs/op bij 1e3
	d := New(0) //                            6.63 ns/op   0 B/op   0 allocs/op bij 1e6
	//                                        80.4 ns/op   0 B/op   0 allocs/op bij 1e3
	rand.Seed(15)
	for i := 0; i < 1000; i++ {
		d.WriteFloat32(rand.Float32())
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k float32
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1000; j++ { // Ik vrees dat hier MLP aan het werk is. En alles is strided, niet random!
			k = d.ReadFloat32(j)
		}
	}
	_ = k
}

func BenchmarkReadFloat32(b *testing.B) { // 32.3 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputFloat32 {
		d.WriteFloat32(x)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputFloat32 {
			d.ReadFloat32(uint32(j))
		}
	}
}

func BenchmarkReadFloat64x(b *testing.B) { // 10.5 ns/op   0 B/op   0 allocs/op bij 1e3 strided
	d := New(0) //                             6.8 ns/op   0 B/op   0 allocs/op bij 1e6 strided
	//                                         164 ns/op   0 B/op   0 allocs/op bij 1e3 strided
	rand.Seed(15)
	for i := 0; i < 1_000; i++ {
		d.WriteFloat64(rand.Float64())
	}

	b.ReportAllocs()
	b.ResetTimer()

	var k float64
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < 1_000; j++ {
			k = d.ReadFloat64(j)
		}
	}
	_ = k
}

func BenchmarkReadFloat64(b *testing.B) { // 61.2 ns/op   0 B/op   0 allocs/op
	d := New(0)
	for _, x := range inputFloat64 {
		d.WriteFloat64(x)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range inputFloat64 {
			d.ReadFloat64(uint32(j))
		}
	}
}
