package dac

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

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

// func TestLength(t *testing.T) {
// 	for i := 0; i < 65; i++ {
// 		a := uint64(1)<<i - 1
// 		lBits := bits.Len64(a)
// 		lBytes := (63 - bits.LeadingZeros64(a)) >> 3
// 		t.Errorf("i: %d, lBits: %d, lBytes: %d\n", i, lBits, lBytes)
// 	}
// }

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
	d.Close()

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
	d.Close()

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
	d.Close()

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
	d.Close()

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
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadUint64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestWriteBoolList(t *testing.T) {
	const n = 100
	numbers := make([]bool, n)

	rand.Seed(15)
	for i := range numbers {
		if rand.Int31n(2) == 1 {
			numbers[i] = true
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteBoolList(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadBool(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %v, want: %v, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteUint8List(t *testing.T) {
	const n = 100
	numbers := make([]uint8, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	for i := range numbers {
		numbers[i] = uint8(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteUint8List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadUint8(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteUint16List(t *testing.T) {
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

	d.WriteUint16List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadUint16(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteUint32List(t *testing.T) {
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

	d.WriteUint32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadUint32(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteUint64List(t *testing.T) {
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

	d.WriteUint64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadUint64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
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
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt8(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt16(t *testing.T) { // TODO: errors
	const n = 100

	numbers := make([]int16, n)

	// rand.Seed(15)
	// for i := range numbers {
	// 	numbers[i] = int16(rand.Int31n(math.MaxInt16+1) + math.MinInt16) // aanpassen
	// }

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range numbers {
		d.WriteInt16(v)
		_ = i
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt16(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteInt32(t *testing.T) { // TODO: errors
	const n = 1_000

	numbers := make([]int32, n)

	// rand.Seed(15)
	// for i := range numbers { // Best ook zipf gebruiken!!!
	// 	numbers[i] = int32(rand.Int63n(math.MaxInt32+1) + math.MinInt32)
	// }

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteInt32(v)
	}
	d.Close()

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
	d.Close()

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
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteFloat32(v)
	}
	d.Close()

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
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteFloat64(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat642(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteFloat642(t *testing.T) {
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
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat642(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteDate(t *testing.T) {
	const n = 100

	dates := make([]time.Time, n)

	rand.Seed(15)
	for i := range dates {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dates[i] = time.Unix(s, rand.Int63n(1_000_000_000))
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range dates {
		d.WriteDate(v)
	}
	d.Close()

	for k, want := range dates {
		got, err := d.ReadDate(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestWriteInt8List(t *testing.T) {
	const n = 100
	numbers := make([]int8, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)

	for i := range numbers {
		numbers[i] = int8(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt8List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt8(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteInt16List(t *testing.T) {
	const n = 100
	numbers := make([]int16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt16List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt16(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteInt32List(t *testing.T) {
	const n = 100
	numbers := make([]int32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt32(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteInt64List(t *testing.T) {
	const n = 100
	numbers := make([]int64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadInt64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteFloat32List(t *testing.T) {
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
	d.WriteFloat32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat32(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %f, want: %f, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteFloat64List(t *testing.T) {
	const n = 100

	numbers := make([]float64, n)
	values := make([]float64, n)

	rand.Seed(15)
	for i := range numbers {
		numbers[i] = rand.Float64()
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	d.WriteFloat64List(numbers)
	d.Close()

	d.ReadFloat64List(values)

	for k, want := range numbers {
		got, err := d.ReadFloat64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %f, want: %f, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteDateList(t *testing.T) {
	const n = 100
	dates := make([]time.Time, n)

	rand.Seed(15)
	for i := range dates {
		dates[i] = time.Unix(rand.Int63n(math.MaxInt64), 0)
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteDateList(dates)
	d.Close()

	for k, want := range dates {
		got, err := d.ReadDate(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %v, want: %v, err: %s\n", k, got, want, err)
		}
	}
}

func TestReadBoolList(t *testing.T) {
	const n = 100
	numbers := make([]bool, n)
	values := make([]bool, n)

	for i := range numbers {
		if rand.Int31n(2) == 1 {
			numbers[i] = true
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteBoolList(numbers)
	d.ReadBoolList(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadUint8List(t *testing.T) {
	const n = 100
	numbers := make([]uint8, n)
	values := make([]uint8, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	for i := range numbers {
		numbers[i] = uint8(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteUint8List(numbers)
	d.ReadUint8List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadUint16List(t *testing.T) {
	const n = 100
	numbers := make([]uint16, n)
	values := make([]uint16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	for i := range numbers {
		numbers[i] = uint16(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteUint16List(numbers)
	d.ReadUint16List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadUint32List(t *testing.T) {
	const n = 100
	numbers := make([]uint32, n)
	values := make([]uint32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	for i := range numbers {
		numbers[i] = uint32(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteUint32List(numbers)
	d.ReadUint32List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadList(t *testing.T) { // TODO: Fout
	const n = 100
	numbers := make([]uint64, n)
	values := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)
	d.ReadList(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestRead64List(t *testing.T) { // TODO: Fout
	const n = 100
	numbers := make([]uint64, n)
	values := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)
	d.Read64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

// func TestReadUint64List(t *testing.T) { // TODO: Fout
// 	const n = 100
// 	numbers := make([]uint64, n)
// 	values := make([]uint64, n)

// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

// 	for i := range numbers {
// 		numbers[i] = zipf.Uint64()
// 	}

// 	d := From(numbers)
// 	d.ReadUint64List(values)

// 	for k, want := range numbers {
// 		got := values[k]
// 		if got != want {
// 			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
// 		}
// 	}
// }

func TestReadInt8List(t *testing.T) {
	const n = 100
	numbers := make([]int8, n)
	values := make([]int8, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	for i := range numbers {
		numbers[i] = int8(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt8List(numbers)
	d.ReadInt8List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadInt16List(t *testing.T) { // TODO: errors
	const n = 100
	numbers := make([]int16, n)
	values := make([]int16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt16List(numbers)
	d.ReadInt16List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadInt32List(t *testing.T) { // TODO: errors
	const n = 100
	numbers := make([]int32, n)
	values := make([]int32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteInt32List(numbers)
	d.ReadInt32List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadInt64List(t *testing.T) { // TODO: errors
	const n = 100
	numbers := make([]int64, n)
	values := make([]int64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
		d.WriteInt64(numbers[i])
	}

	d.ReadInt64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadFloat32List(t *testing.T) {
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

	d.WriteFloat32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %f, want: %f\n", k, got, want)
		}
	}
}

func TestReadFloat64List(t *testing.T) {
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

	d.WriteFloat64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %f, want: %f\n", k, got, want)
		}
	}
}

func TestReadDateList(t *testing.T) { // TODO
	const n = 100
	numbers := make([]int64, n)
	values := make([]int64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
		d.WriteInt64(numbers[i])
	}
	d.Close()

	d.ReadInt64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestScan(t *testing.T) {
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	results := map[uint64]int{}
	for i := len(numbers) - 1; i >= 0; i-- {
		results[numbers[i]] = i
	}

	d := From(numbers)

	for i, v := range numbers {
		got := d.Scan(v)
		want, ok := results[v]
		if !ok || got != want {
			t.Errorf("%d: Search %d - got: %d, want: %d\n", i, v, got, want)
		}
	}
}

func TestSearch(t *testing.T) {
	const n = 100

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	d := From(numbers)

	var prev uint64
	var prevIdx int
	for wantIdx, v := range numbers {
		if v == prev {
			wantIdx = prevIdx
		} else {
			prevIdx = wantIdx
			prev = v
		}

		gotIdx, _ := d.Search(v)
		if gotIdx != wantIdx {
			t.Errorf("gotIdx: %d, wantIdx: %d\n", gotIdx, wantIdx)
		}

	}
}

func BenchmarkWriteBool(b *testing.B) { // 5.23 ns/op   0 B/op   0 allocs/op
	const n = 1_000

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

func BenchmarkWriteUint8(b *testing.B) { // 2.20 ns/op   0 B/op   0 allocs/op
	const n = 1_000

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

func BenchmarkWriteUint16(b *testing.B) { // 7.66 ns/op   0 B/op   0 allocs/op
	const n = 1_000

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
	const n = 1_000

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
			d.WriteUint64(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteUint643(b *testing.B) { // 8.79 ns/op   0 B/op   0 allocs/op
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

func BenchmarkWriteBoolList(b *testing.B) { // 2.10 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]bool, n)

	rand.Seed(15)
	for i := range numbers {
		if rand.Int31n(2) == 1 {
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
		d.WriteBoolList(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUint8List(b *testing.B) { // 0.06 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
		d.WriteUint8List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUint16List(b *testing.B) { // 3.97 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
		d.WriteUint16List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUint32List(b *testing.B) { // 4.56 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
		d.WriteUint32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUint64List(b *testing.B) { // 5.33 ns/op    0 B/op    0 allocs/op
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
		d.WriteUint64List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUvarintList(b *testing.B) { // 6.63 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	buf := make([]byte, binary.MaxVarintLen64)
	results := []byte{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			n := binary.PutUvarint(buf, x)
			results = append(results, buf[:n]...)
		}
		results = results[:0]
	}
}

func BenchmarkWriteInt8(b *testing.B) { // 2.11 ns/op   0 B/op   0 allocs/op
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

func BenchmarkWriteInt16(b *testing.B) { // 8.19 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int16, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
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
			d.WriteInt16(v)
		}
		d.Reset()
	}
}

// // TODOs
// // Moet ik ook het int datatype ondersteunen???Misschien wel!
// // BenchmarkWrite is afhankelijk van de lengte. Testen op verschillende lengtes, maar pas wanneer rank volledig is geimplementeerd!!!

func BenchmarkWriteInt32(b *testing.B) { // 9.36 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
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
			d.WriteInt32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteInt64(b *testing.B) { // 10.5 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
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

func BenchmarkWriteDate(b *testing.B) { // 33.1 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	dates := make([]time.Time, n)

	rand.Seed(15)
	for i := range dates {
		dates[i] = time.Unix(rand.Int63n(math.MaxInt64), 0)
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range dates {
			d.WriteDate(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteInt8List(b *testing.B) { // 2.18 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]int8, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int8(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteInt8List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteInt16List(b *testing.B) { // 3.36 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]int16, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteInt16List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteInt32List(b *testing.B) { // 4.24 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]int32, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteInt32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteInt64List(b *testing.B) { // 5.03 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]int64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteInt64List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteVarintList(b *testing.B) { // 7.00 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]int64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	buf := make([]byte, binary.MaxVarintLen64)
	results := []byte{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, x := range numbers {
			n := binary.PutVarint(buf, x)
			results = append(results, buf[:n]...)
		}
		results = results[:0]
	}
}

func BenchmarkWriteFloat32List(b *testing.B) { // 15.6 ns/op   0 B/op   0 allocs/op
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
		d.WriteFloat32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteFloat64List(b *testing.B) { // 23.8 ns/op   0 B/op   0 allocs/op
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
		d.WriteFloat64List(numbers)
		d.Reset()
	}
}
func BenchmarkWriteDateList(b *testing.B) { // 31.7 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	dates := make([]time.Time, n)

	rand.Seed(15)
	for i := range dates {
		dates[i] = time.Unix(rand.Int63n(math.MaxInt64), 0)
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range dates {
			d.WriteDate(v)
		}
		d.Reset()
	}
}

func BenchmarkReadBool(b *testing.B) { // 0.87 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	rand.Seed(15)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		if a := rand.Int31n(2); a == 1 {
			d.WriteBool(true)
			continue
		}
		d.WriteBool(false)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadBool(k)
		}
	}
}

func BenchmarkReadUint8(b *testing.B) { // 0.87 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint8(zipf.Uint64())
		d.WriteUint8(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint8(k)
		}
	}
}

func BenchmarkReadUint16(b *testing.B) { // 7.44 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint16(zipf.Uint64())
		d.WriteUint16(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint16(k)
		}
	}
}

func BenchmarkReadUint162(b *testing.B) { // 6.04 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint16(zipf.Uint64())
		d.WriteUint16(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint162(k)
		}
	}
}

func BenchmarkReadUint32(b *testing.B) { // 9.33 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint32(zipf.Uint64())
		d.WriteUint32(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint32(k)
		}
	}
}

func BenchmarkReadUint322(b *testing.B) { // 8.82 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint32(zipf.Uint64())
		d.WriteUint32(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint322(k)
		}
	}
}

func BenchmarkReadUint64(b *testing.B) { // 10.9 ns/op    0 B/op    0 allocs/op
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
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint64(k)
		}
	}
}

func BenchmarkReadUint642(b *testing.B) { // 9.59 ns/op    0 B/op    0 allocs/op
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
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadUint642(k)
		}
	}
}

func BenchmarkReadBoolList(b *testing.B) { // 1.23 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	numbers := make([]bool, n)

	rand.Seed(15)
	for i := range numbers {
		if rand.Int31n(2) == 0 {
			numbers[i] = true
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}
	d.WriteBoolList(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadBoolList(numbers)
	}
}

func BenchmarkReadUint8List(b *testing.B) { // 0.60 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
	d.WriteUint8List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadUint8List(numbers)
	}
}

func BenchmarkReadUint16List(b *testing.B) { // 2.86 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
	d.WriteUint16List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadUint16List(numbers)
	}
}

func BenchmarkReadUint32List(b *testing.B) { // 3.38 ns/op    0 B/op    0 allocs/op
	const n = 1_000

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
	d.WriteUint32List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadUint32List(numbers)
	}
}

func BenchmarkReadList(b *testing.B) { // 3.66 ns/op    0 B/op    0 allocs/op
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
	d.WriteUint64List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadList(numbers)
	}
}

func BenchmarkRead64List(b *testing.B) { // 3.64 ns/op    0 B/op    0 allocs/op
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
	d.WriteUint64List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.Read64List(numbers)
	}
}

// func BenchmarkReadUint64List(b *testing.B) { // 3.66 ns/op    0 B/op    0 allocs/op
// 	const n = 1_000

// 	numbers := make([]uint64, n)
// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

// 	for i := range numbers {
// 		numbers[i] = zipf.Uint64()
// 	}

// 	d, err := New(n)
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// 	d.WriteUint64List(numbers)

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		d.ReadUint64List(numbers)
// 	}
// }

func BenchmarkReadInt8(b *testing.B) { // 0.87 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int8(zipf.Uint64())
		d.WriteInt8(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadInt8(k)
		}
	}
}

func BenchmarkReadInt16(b *testing.B) { // 12.2 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int16(zipf.Uint64())
		d.WriteInt16(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadInt16(k)
		}
	}
}

func BenchmarkReadInt32(b *testing.B) { // 15.4 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int32(zipf.Uint64())
		d.WriteInt32(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadInt32(k)
		}
	}
}

func BenchmarkReadInt64(b *testing.B) { // 15.8 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int64(zipf.Uint64())
		d.WriteInt64(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadInt64(k)
		}
	}
}

func BenchmarkReadFloat32(b *testing.B) { // 18.3 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat64(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			d.ReadFloat32(j)
		}
	}
}

func BenchmarkReadFloat64(b *testing.B) { // 19.1 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat64(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			d.ReadFloat64(j)
		}
	}
}

func BenchmarkReadFloat642(b *testing.B) { // 25.2 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat64(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			d.ReadFloat642(j)
		}
	}
}

func BenchmarkReadDate(b *testing.B) { // 61.4 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	rand.Seed(15)
	for i := 0; i < n; i++ {
		dt := time.Unix(rand.Int63n(math.MaxInt64), 0)
		d.WriteDate(dt)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadDate(k)
		}
	}
}

func BenchmarkReadInt8List(b *testing.B) { // 1.18 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int8, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int8(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteInt8List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadInt8List(numbers)

	}
}

func BenchmarkReadInt16List(b *testing.B) { // 6.89 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int16, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int16(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteInt16List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadInt16List(numbers)
	}
}

func BenchmarkReadInt32List(b *testing.B) { // 7.43 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int32, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteInt32List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadInt32List(numbers)
	}
}

func BenchmarkReadInt64List(b *testing.B) { // 7.64 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]int64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	d.WriteInt64List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadInt64List(numbers)
	}
}

func BenchmarkReadFloat32List(b *testing.B) { // 11.4 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat64(v)
	}
	d.Close()

	numbers := make([]float32, n)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadFloat32List(numbers)
	}
}

func BenchmarkReadFloat64List(b *testing.B) { // 11.4 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat64(v)
	}
	d.Close()

	numbers := make([]float64, n)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadFloat64List(numbers)
	}
}

func BenchmarkReadDateList(b *testing.B) { // 13.0 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	rand.Seed(15)
	for i := 0; i < n; i++ {
		dt := time.Unix(rand.Int63n(math.MaxInt64), 0) // nanosecs ook invullen
		d.WriteDate(dt)
	}
	d.Close()

	numbers := make([]time.Time, n)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadDateList(numbers)
	}
}

func BenchmarkScan(b *testing.B) { // 289 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range numbers {
			d.Scan(v)
		}
	}
}

func BenchmarkSearch(b *testing.B) { // 41.5 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})

	d := From(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := uint64(0); k < n; k++ {
			d.Search(k)
		}
	}
}

func BenchmarkRank(b *testing.B) { // 5.08 ns/op    0 B/op    0 allocs/op    n = 1_000
	const n = 1_000 //                5.16 ns/op    0 B/op    0 allocs/op    n = 1_000_000

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

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// NOT random access
		for j := 0; j < n; j++ {
			d.rank(0, j)
		}
	}
}

func TestEncZigzag(t *testing.T) {
	// for i := -math.MaxInt8; i < math.MaxInt8; i++ {
	// 	i8 := int8(i)
	// 	u8 := uint8((i8 << 1) ^ (i8 >> 7))
	// 	got := int8((u8 >> 1) ^ -(u8 & 1))
	// 	if got != i8 {
	// 		t.Error("Kaboem!")
	// 	}
	// }

	// for i := -math.MaxInt16; i < math.MaxInt16; i++ {
	// 	i16 := int16(i)
	// 	i64 := int64(i)
	// 	u16 := uint16((i16 << 1) ^ (i16 >> 15))
	// 	u64 := uint64((i16 << 1) ^ (i16 >> 15))
	// 	u65 := uint64((i64 << 1) ^ (i64 >> 15))
	// 	got := int16((u16 >> 1) ^ -(u16 & 1))
	// 	got64 := int16((u64 >> 1) ^ -(u64 & 1))
	// 	got65 := int16((u65 >> 1) ^ -(u65 & 1))
	// 	if got != i16 || got64 != i16 || got65 != i16 {
	// 		t.Errorf("want: %d, got: %d, got64: %d ,got65: %d\n", i16, got, got64, got65)
	// 	}
	// }

	// for i := -math.MaxInt32; i < math.MaxInt32; i++ {
	// 	i32 := int32(i)
	// 	u32 := uint32((i32 << 1) ^ (i32 >> 31))
	// 	got := int32((u32 >> 1) ^ -(u32 & 1))
	// 	if got != i32 {
	// 		t.Error("Kaboem!")
	// 	}
	// }
}

// func TestEncZigzag(t *testing.T) {
// 	const n = 1_000

// 	numbers := make([]int64, n)

// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)

// 	for i := 0; i < n; i++ {
// 		numbers[i] = int64(zipf.Uint64())
// 		if rand.Int31n(2) == 1 {
// 			numbers[i] *= -1
// 		}
// 	}

// 	var uv1, uv2 uint64
// 	for _, num := range numbers {
// 		uv1 = uint64(num) << 1
// 		if num < 0 {
// 			uv1 = ^uv1
// 		}
// 		uv2 = uint64((num << 1) ^ (num >> 63))

// 		if uv1 != uv2 {
// 			t.Error("Kaboem!")
// 		}
// 	}
// }

// func TestDecZigzag(t *testing.T) {
// 	const n = 1_000

// 	numbers := make([]uint64, n)

// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

// 	for i := 0; i < n; i++ {
// 		numbers[i] = zipf.Uint64()
// 	}

// 	var v1, v2 int64
// 	for _, uv := range numbers {
// 		v1 = int64(uv >> 1)
// 		if uv&1 != 0 {
// 			v1 = ^v1
// 		}
// 		v2 = int64((uv >> 1) ^ -(uv & 1))

// 		if v1 != v2 {
// 			t.Error("Kaboem!")
// 		}
// 	}
// }

// func BenchmarkEncZigzag(b *testing.B) {
// 	const n = 1_000

// 	numbers := make([]int64, n)

// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)

// 	for i := 0; i < n; i++ {
// 		numbers[i] = int64(zipf.Uint64())
// 		if rand.Int31n(2) == 1 {
// 			numbers[i] *= -1
// 		}
// 	}

// 	b.Run("1", func(b *testing.B) {
// 		var uv uint64
// 		for i := 0; i < b.N; i++ {
// 			for _, v := range numbers {
// 				uv = uint64(v) << 1
// 				if v < 0 {
// 					uv = ^uv
// 				}
// 			}
// 		}
// 		_ = uv
// 	})

// 	b.Run("2", func(b *testing.B) {
// 		var uv uint64
// 		for i := 0; i < b.N; i++ {
// 			for _, v := range numbers {
// 				uv = uint64((v << 1) ^ (v >> 63))
// 			}
// 		}
// 		_ = uv
// 	})

// 	b.Run("3", func(b *testing.B) { // Not zigzag!!!
// 		var uv uint64
// 		for i := 0; i < b.N; i++ {
// 			for _, v := range numbers {
// 				uv = *(*uint64)(unsafe.Pointer(&v))
// 			}
// 		}
// 		_ = uv
// 	})
// }

// func BenchmarkDecZigzag(b *testing.B) {
// 	const n = 1_000

// 	numbers := make([]uint64, n)

// 	r := rand.New(rand.NewSource(15))
// 	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

// 	for i := 0; i < n; i++ {
// 		numbers[i] = zipf.Uint64()
// 	}

// 	b.Run("1", func(b *testing.B) {
// 		var v int64
// 		for i := 0; i < b.N; i++ {
// 			for _, uv := range numbers {
// 				v := int64(uv >> 1)
// 				if uv&1 != 0 {
// 					v = ^v
// 				}
// 			}
// 		}
// 		_ = v
// 	})

// 	b.Run("2", func(b *testing.B) {
// 		var v int64
// 		for i := 0; i < b.N; i++ {
// 			for _, uv := range numbers {
// 				v = int64((uv >> 1) ^ -(uv & 1))
// 			}
// 		}
// 		_ = v
// 	})

// 	b.Run("3", func(b *testing.B) { // Not zigzag!!!
// 		var v int64
// 		for i := 0; i < b.N; i++ {
// 			for _, uv := range numbers {
// 				v = *(*int64)(unsafe.Pointer(&uv))
// 			}
// 		}
// 		_ = v
// 	})
// }
