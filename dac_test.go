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
	const n = 1_000
	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)

	for k, want := range numbers {
		got, err := d.ReadU64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestReadWriteBool(t *testing.T) {
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

func TestReadWriteU8(t *testing.T) {
	const n = 1_000

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
		d.WriteU8(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU8(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteU16(t *testing.T) {
	const n = 1_000

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
		d.WriteU16(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU16(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteU32(t *testing.T) {
	const n = 1_000

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
		d.WriteU32(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteU64(t *testing.T) {
	const n = 1_000

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
		d.WriteU64(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestRemovea(t *testing.T) {
	const n = 1_025

	numbers := make([]uint64, n)

	for i := range numbers {
		numbers[i] = 5
	}

	d := From(numbers)

	for range numbers {
		d.RemoveAt(0)
	}

	for i := range numbers {
		got, err := d.ReadU64(i)
		want := uint64(5)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestRemoveb(t *testing.T) {
	const n = 1_000

	numbers := make([]uint64, n)

	for i := range numbers {
		numbers[i] = uint64(300 * i)
	}

	d := From(numbers)

	for range numbers {
		d.RemoveAt(0)
	}

	for i := range numbers {
		got, err := d.ReadU64(i)
		want := uint64(5)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestRemove(t *testing.T) {
	const n = 100_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
	for i := 0; i < n; i++ {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)

	for i := 0; i < n; i++ {
		d.RemoveAt(0)
	}

	d.WriteU64List(numbers)
	d.Close()

	for i := n - 1; 0 <= i; i-- {
		d.RemoveAt(i)
	}

	d.Close()
}

func TestWriteU64Ata0(t *testing.T) {
	const n = 10

	numbers := make([]uint64, n)

	for i := range numbers {
		numbers[i] = uint64(300 * (i + 1))
	}

	d := From(numbers)

	for range numbers {
		d.WriteU64At(0, 5)
	}

	for i := range numbers {
		got, err := d.ReadU64(i)
		want := uint64(5)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestWriteU64Ata(t *testing.T) {
	const n = 250

	numbers := make([]uint64, n)

	for i := range numbers {
		numbers[i] = uint64(300 * (i + 1))
	}

	d := From(numbers)

	for i := range numbers {
		d.WriteU64At(i, 5)
	}

	for i := range numbers {
		got, err := d.ReadU64(i)
		want := uint64(5)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestWriteU64Atb(t *testing.T) {
	const n = 1_000

	numbers := make([]uint64, n)

	for i := range numbers {
		numbers[i] = 5
	}

	d := From(numbers)

	for i := range numbers {
		d.WriteU64At(i, 300*uint64(i))
	}

	for i := range numbers {
		got, err := d.ReadU64(i)
		want := 300 * uint64(i)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestWriteU64At(t *testing.T) {
	const n = 1_000

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := 0; i < n; i++ {
		d.WriteU64(zipf.Uint64())
	}
	d.Close()

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	for i := range numbers {
		d.WriteU64At(i, numbers[i])
	}

	for i, want := range numbers {
		got, err := d.ReadU64(i)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", i, got, want)
		}
	}
}

func TestWriteBoolList(t *testing.T) {
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

func TestWriteU8List(t *testing.T) {
	const n = 1_000
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

	d.WriteU8List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU8(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteU16List(t *testing.T) {
	const n = 1_000
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

	d.WriteU16List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU16(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteU32List(t *testing.T) {
	const n = 1_000
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

	d.WriteU32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU32(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteU64List(t *testing.T) {
	const n = 1_000
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

	d.WriteU64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadU64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestReadWriteI8(t *testing.T) {
	const n = 1_000

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
		d.WriteI8(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI8(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteI16(t *testing.T) {
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
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteI16(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI16(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteI32(t *testing.T) { // TODO: errors
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
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteI32(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI32(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteI64(t *testing.T) {
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
		t.Fatal(err)
	}

	for _, v := range numbers {
		d.WriteI64(v)
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteFloat32(t *testing.T) {
	const n = 1_000

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	numbers := make([]float32, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
		d.WriteFloat32(numbers[i])
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
	const n = 1_000

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	numbers := make([]float64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
		d.WriteFloat64(numbers[i])
	}
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat64(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestReadWriteDateTime(t *testing.T) {
	const n = 1_000

	dateTimes := make([]time.Time, n)

	rand.Seed(15)
	for i := range dateTimes {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dateTimes[i] = time.Unix(s, rand.Int63n(1_000_000_000))
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range dateTimes {
		d.WriteDateTime(v)
	}
	d.Close()

	for k, want := range dateTimes {
		got, err := d.ReadDateTime(k)
		if got != want || err != nil {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
		}
	}
}

func TestWriteI8List(t *testing.T) {
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
		t.Fatal(err)
	}

	d.WriteI8List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI8(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteI16List(t *testing.T) {
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
		t.Fatal(err)
	}

	d.WriteI16List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI16(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteI32List(t *testing.T) {
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
		t.Fatal(err)
	}

	d.WriteI32List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI32(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteI64List(t *testing.T) {
	const n = 1_000
	numbers := make([]int64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)
	rand.Seed(15)

	for i := range numbers {
		numbers[i] = int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteI64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadI64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %d, want: %d, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteFloat32List(t *testing.T) {
	const n = 1_000

	numbers := make([]float32, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
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
	const n = 1_000

	numbers := make([]float64, n)
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	d.WriteFloat64List(numbers)
	d.Close()

	for k, want := range numbers {
		got, err := d.ReadFloat64(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %f, want: %f, err: %s\n", k, got, want, err)
		}
	}
}

func TestWriteDateTimeList(t *testing.T) {
	const n = 1_000
	dateTimes := make([]time.Time, n)

	rand.Seed(15)
	for i := range dateTimes {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dateTimes[i] = time.Unix(s, rand.Int63n(1_000_000_000))
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteDateTimeList(dateTimes)
	d.Close()

	for k, want := range dateTimes {
		got, err := d.ReadDateTime(k)
		if err != nil || got != want {
			t.Errorf("k: %d - got: %v, want: %v, err: %s\n", k, got, want, err)
		}
	}
}

func TestReadBoolList(t *testing.T) {
	const n = 1_000
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

func TestReadU8List(t *testing.T) {
	const n = 1_000
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

	d.WriteU8List(numbers)
	d.ReadU8List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadU16List(t *testing.T) {
	const n = 1_000
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

	d.WriteU16List(numbers)
	d.ReadU16List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadU32List(t *testing.T) {
	const n = 1_000
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

	d.WriteU32List(numbers)
	d.ReadU32List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadU64List(t *testing.T) {
	const n = 1_000
	numbers := make([]uint64, n)
	values := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	d := From(numbers)
	d.ReadU64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadI8List(t *testing.T) {
	const n = 1_000
	numbers := make([]int8, n)
	values := make([]int8, n)

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
		t.Fatal(err)
	}

	d.WriteI8List(numbers)
	d.ReadI8List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadI16List(t *testing.T) {
	const n = 1_000
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

	d.WriteI16List(numbers)
	d.ReadI16List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadI32List(t *testing.T) {
	const n = 1_000
	numbers := make([]int32, n)
	values := make([]int32, n)

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
		t.Fatal(err)
	}

	d.WriteI32List(numbers)
	d.ReadI32List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadI64List(t *testing.T) {
	const n = 1_000
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
		d.WriteI64(numbers[i])
	}
	d.Close()

	d.ReadI64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %d, want: %d\n", k, got, want)
		}
	}
}

func TestReadFloat32List(t *testing.T) {
	const n = 1_000
	numbers := make([]float32, n)
	values := make([]float32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	// d.WriteFloat32List(numbers)
	for _, v := range numbers {
		d.WriteFloat32(v)
	}
	d.Close()

	d.ReadFloat32List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want || err != nil {
			t.Errorf("k: %d - got: %f, want: %f\n", k, got, want)
		}
	}
}

func TestReadFloat64List(t *testing.T) {
	const n = 1_000
	numbers := make([]float64, n)
	values := make([]float64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			numbers[i] *= -1
		}
	}

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	d.WriteFloat64List(numbers)
	d.Close()

	d.ReadFloat64List(values)

	for k, want := range numbers {
		got := values[k]
		if got != want || err != nil {
			t.Errorf("k: %d - got: %f, want: %f\n", k, got, want)
		}
	}
}

func TestReadDateTimeList(t *testing.T) {
	const n = 1_000
	dateTimes := make([]time.Time, n)
	values := make([]time.Time, n)

	d, err := New(n)
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(15)
	for i := range dateTimes {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dateTimes[i] = time.Unix(s, rand.Int63n(1_000_000_000))
		d.WriteDateTime(dateTimes[i])
	}
	d.Close()

	d.ReadDateTimeList(values)

	for k, want := range dateTimes {
		got := values[k]
		if got != want {
			t.Errorf("k: %d - got: %v, want: %v\n", k, got, want)
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

func BenchmarkTestFrom(b *testing.B) { // 8.42 ns/op   5400 B/op   53 allocs/op
	const n = 1_000

	numbers := make([]uint64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d := From(numbers)
		d.Reset()
	}
}

func BenchmarkWriteBool(b *testing.B) { // 5.00 ns/op   0 B/op   0 allocs/op
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

func BenchmarkWriteU8(b *testing.B) { // 2.20 ns/op   0 B/op   0 allocs/op
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
			d.WriteU8(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteU16(b *testing.B) { // 5.85 ns/op   0 B/op   0 allocs/op
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
			d.WriteU16(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteU32(b *testing.B) { // 8.69 ns/op   0 B/op   0 allocs/op
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
			d.WriteU32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteU64(b *testing.B) { // 8.68 ns/op   0 B/op   0 allocs/op
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
			d.WriteU64(v)
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

func BenchmarkWriteU8List(b *testing.B) { // 0.06 ns/op    0 B/op    0 allocs/op
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
		d.WriteU8List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteU16List(b *testing.B) { // 2.57 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                        2.31 ns/op    0 B/op    0 allocs/op

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
		d.WriteU16List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteU32List(b *testing.B) { // 3.86 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                        3.82 ns/op    0 B/op    0 allocs/op

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
		d.WriteU32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteU64List(b *testing.B) { // 4.59 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                        4.14 ns/op    0 B/op    0 allocs/op

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
		d.WriteU64List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteUvarintList(b *testing.B) { // 6.48 ns/op    0 B/op    0 allocs/op
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

func BenchmarkWriteI8(b *testing.B) { // 2.11 ns/op   0 B/op   0 allocs/op
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
			d.WriteI8(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteI16(b *testing.B) { // 8.19 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                    6.35 ns/op   0 B/op   0 allocs/op

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
			d.WriteI16(v)
		}
		d.Reset()
	}
}

// TODOs
// Moet ik ook het int datatype ondersteunen???Misschien wel!

func BenchmarkWriteI32(b *testing.B) { // 9.36 ns/op   0 B/op   0 allocs/op
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
			d.WriteI32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteI64(b *testing.B) { // 10.5 ns/op   0 B/op   0 allocs/op
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
			d.WriteI64(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteFloat32(b *testing.B) { // 14.9 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	numbers := make([]float32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float32(zipf.Uint64())
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
			d.WriteFloat32(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteFloat64(b *testing.B) { // 28.3 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                        13.6 ns/op   0 B/op   0 allocs/op

	numbers := make([]float64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float64(zipf.Uint64())
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
			d.WriteFloat64(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteDateTime(b *testing.B) { // 30.7 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	dateTimes := make([]time.Time, n)

	rand.Seed(15)
	for i := 0; i < n; i++ {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dateTimes[i] = time.Unix(s, rand.Int63n(1_000_000_000))
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, v := range dateTimes {
			d.WriteDateTime(v)
		}
		d.Reset()
	}
}

func BenchmarkWriteI8List(b *testing.B) { // 2.18 ns/op    0 B/op    0 allocs/op
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
		d.WriteI8List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteI16List(b *testing.B) { // 2.47 ns/op    0 B/op    0 allocs/op
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
		d.WriteI16List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteI32List(b *testing.B) { // 4.24 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                        3.96 ns/op    0 B/op    0 allocs/op

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
		d.WriteI32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteI64List(b *testing.B) { // 5.03 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                        4.50 ns/op    0 B/op    0 allocs/op

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
		d.WriteI64List(numbers)
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
	const n = 1_000 //                            6.99 ns/op   0 B/op   0 allocs/op
	//                                            6.37 ns/op   0 B/op   0 allocs/op
	numbers := make([]float32, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float32(zipf.Uint64())
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
		d.WriteFloat32List(numbers)
		d.Reset()
	}
}

func BenchmarkWriteFloat64List(b *testing.B) { // 23.8 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                            8.41 ns/op   0 B/op   0 allocs/op
	//                                            7.78 ns/op   0 B/op   0 allocs/op
	numbers := make([]float64, n)

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		numbers[i] = float64(zipf.Uint64())
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
		d.WriteFloat64List(numbers)
		d.Reset()
	}
}
func BenchmarkWriteDateTimeList(b *testing.B) { // 29.9 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                             29.9 ns/op    0 B/op    0 allocs/op

	dateTimes := make([]time.Time, n)

	rand.Seed(15)
	for i := 0; i < n; i++ {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dateTimes[i] = time.Unix(s, rand.Int63n(1_000_000_000))
	}

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.WriteDateTimeList(dateTimes)
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

func BenchmarkReadU8(b *testing.B) { // 0.87 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint8)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint8(zipf.Uint64())
		d.WriteU8(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadU8(k)
		}
	}
}

func BenchmarkReadU16(b *testing.B) { // 6.21 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                   6.21 ns/op   0 B/op   0 allocs/op

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint16)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint16(zipf.Uint64())
		d.WriteU16(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadU16(k)
		}
	}
}

func BenchmarkReadU32(b *testing.B) { // 9.33 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                   7.60 ns/op   0 B/op   0 allocs/op

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint32)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := uint32(zipf.Uint64())
		d.WriteU32(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadU32(k)
		}
	}
}

func BenchmarkReadU64(b *testing.B) { // 10.9 ns/op    0 B/op    0 allocs/op
	const n = 1_000 //                   11.0 ns/op    0 B/op    0 allocs/op
	//                                   11.0 ns/op    0 B/op    0 allocs/op
	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		d.WriteU64(zipf.Uint64())
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadU64(k)
		}
	}
}

func BenchmarkReadBoolList(b *testing.B) { // 1.18 ns/op    0 B/op    0 allocs/op
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

func BenchmarkReadU8List(b *testing.B) { // 0.60 ns/op    0 B/op    0 allocs/op
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
	d.WriteU8List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadU8List(numbers)
	}
}

func BenchmarkReadU16List(b *testing.B) { // 2.34 ns/op    0 B/op    0 allocs/op
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
	d.WriteU16List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadU16List(numbers)
	}
}

func BenchmarkReadU32List(b *testing.B) { // 3.38 ns/op    0 B/op    0 allocs/op
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
	d.WriteU32List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadU32List(numbers)
	}
}

func BenchmarkReadU64List(b *testing.B) { // 3.66 ns/op    0 B/op    0 allocs/op
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
	d.WriteU64List(numbers)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadU64List(numbers)
	}
}

func BenchmarkReadUvarintList(b *testing.B) { // 8.80 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	buf := make([]byte, binary.MaxVarintLen64)
	results := []byte{}
	for i := 0; i < n; i++ {
		n := binary.PutUvarint(buf, zipf.Uint64())
		results = append(results, buf[:n]...)
	}
	backup := results

	b.ReportAllocs()
	b.ResetTimer()

	var v uint64
	for i := 0; i < b.N; i++ {
		var l int
		for len(results) != 0 {
			v, l = binary.Uvarint(results)
			results = results[l:]
		}
		results = backup
	}
	_ = v
}

func BenchmarkReadI8(b *testing.B) { // 0.87 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt8)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int8(zipf.Uint64())
		d.WriteI8(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadI8(k)
		}
	}
}

func BenchmarkReadI16(b *testing.B) { // 12.2 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                   8.28 ns/op   0 B/op   0 allocs/op

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt16)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int16(zipf.Uint64())
		d.WriteI16(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadI16(k)
		}
	}
}

func BenchmarkReadI32(b *testing.B) { // 15.4 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                   10.9 ns/op   0 B/op   0 allocs/op

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt32)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int32(zipf.Uint64())
		d.WriteI32(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadI32(k)
		}
	}
}

func BenchmarkReadI64(b *testing.B) { // 15.8 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                   11.9 ns/op   0 B/op   0 allocs/op

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < n; i++ {
		v := int64(zipf.Uint64())
		d.WriteI64(v)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadI64(k)
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
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat32(v)
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
	const n = 1_000 //                       23.1 ns/op    0 B/op    0 allocs/op

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)
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

func BenchmarkReadDateTime(b *testing.B) { // 68.7 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	rand.Seed(15)
	for i := 0; i < n; i++ {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dt := time.Unix(s, rand.Int63n(1_000_000_000))
		d.WriteDateTime(dt)
	}
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < n; k++ {
			d.ReadDateTime(k)
		}
	}
}

func BenchmarkReadI8List(b *testing.B) { // 1.18 ns/op   0 B/op   0 allocs/op
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

	d.WriteI8List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadI8List(numbers)

	}
}

func BenchmarkReadI16List(b *testing.B) { // 6.89 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                       6.50 ns/op   0 B/op   0 allocs/op

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

	d.WriteI16List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadI16List(numbers)
	}
}

func BenchmarkReadI32List(b *testing.B) { // 7.43 ns/op   0 B/op   0 allocs/op
	const n = 1_000 //                       7.38 ns/op   0 B/op   0 allocs/op

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

	d.WriteI32List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadI32List(numbers)
	}
}

func BenchmarkReadI64List(b *testing.B) { // 7.64 ns/op   0 B/op   0 allocs/op
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

	d.WriteI64List(numbers)
	d.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadI64List(numbers)
	}
}

func BenchmarkReadVarintList(b *testing.B) { // 13.0 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)
	results := []byte{}
	buf := make([]byte, binary.MaxVarintLen64)

	for i := 0; i < n; i++ {
		x := int64(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			x = -x
		}
		n := binary.PutVarint(buf, x)
		results = append(results, buf[:n]...)
	}
	backup := results

	b.ReportAllocs()
	b.ResetTimer()

	var v int64
	for i := 0; i < b.N; i++ {
		var l int
		for len(results) != 0 {
			v, l = binary.Varint(results)
			results = results[l:]
		}
		results = backup
	}
	_ = v
}

func BenchmarkReadFloat32List(b *testing.B) { // 8.00 ns/op   0 B/op   0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxInt64)
	rand.Seed(15)

	for i := 0; i < n; i++ {
		v := float32(zipf.Uint64())
		if rand.Int31n(2) == 1 {
			v = -v
		}
		d.WriteFloat32(v)
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

func BenchmarkReadDateTimeList(b *testing.B) { // 29.5 ns/op    0 B/op    0 allocs/op
	const n = 1_000

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	rand.Seed(15)
	for i := 0; i < n; i++ {
		s := rand.Int63n(math.MaxInt32)
		if rand.Int31n(2) == 1 {
			s = -s
		}
		dt := time.Unix(s, rand.Int63n(1_000_000_000))
		d.WriteDateTime(dt)
	}
	d.Close()

	numbers := make([]time.Time, n)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.ReadDateTimeList(numbers)
	}
}

func BenchmarkWriteU64At(b *testing.B) { // 53.0 ns/op    50 B/op    0 allocs/op
	const n = 100 //                        9.31 ns/op    50 B/op    0 allocs/op

	d, err := New(n)
	if err != nil {
		b.Fatal(err)
	}

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	for i := 0; i < n; i++ {
		d.WriteU64(zipf.Uint64())
	}
	d.Close()

	numbers := make([]uint64, n)
	for i := range numbers {
		numbers[i] = zipf.Uint64()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range numbers {
			d.WriteU64At(j, numbers[j])
		}
	}
}

func BenchmarkRemoveFirst(b *testing.B) { // 66.5 ns/op    64 B/op    7 allocs/op
	const n = 1_000 //                       Remove(0), d.WriteU64List(numbers), d.Close()

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
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
		d.WriteU64List(numbers)
		d.Close()
		for range numbers {
			d.RemoveAt(0)
		}
	}
}

func BenchmarkRemoveLast(b *testing.B) { // 41.9 ns/op    64 B/op    7 allocs/op
	const n = 1_000 //                      Remove(0), d.WriteU64List(numbers), d.Close()

	r := rand.New(rand.NewSource(15))
	zipf := rand.NewZipf(r, 1.15, 1, math.MaxUint64)

	numbers := make([]uint64, n)
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
		d.WriteU64List(numbers)
		d.Close()
		for j := len(numbers) - 1; 0 <= j; j-- {
			d.RemoveAt(j)
		}
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
		d.WriteU64(zipf.Uint64())
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
