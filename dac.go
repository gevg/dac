// Package dac implements a compressed dictionary for booleans, integers,
// dates, and floats of all sizes. Compression is obtained by application
// of variable byte codes. Direct access to any value is obtained through
// the utilization of Directly Addressable Codes (DACs). The integer data
// can be searched efficiently when sorted.
package dac

import (
	"errors"
	"math"
	"math/bits"
	"time"
	"unsafe"
)

// nStreams is the maximum number of bytes in the
// variable bytes representation of a data element.
const (
	nStreams64 = 8
	nStreams32 = 4
	nStreams16 = 2
)

// Dict is a dictionary type that stores values in a "Directly Addressable
// Codes" structure. Data is compressed but still provides direct access
// to any value. Moreover, data can be searched efficiently when stored in
// sorted order.
type Dict struct {
	chunks [nStreams64][]byte
	bitArr [nStreams64 - 1][]uint64
	ranks  [nStreams64 - 1][]int
}

// TODO: Serdes van en naar file voorzien?!

// New constructs a dictionary with an initial capacity of n values. Setting
// the capacity is optional but recommended for performance reasons. The
// capacity gets automatically updated when needed.
func New(n ...int) (*Dict, error) {
	var m int
	if len(n) != 0 {
		if m = n[0]; m < 0 {
			return nil, errors.New("dac: number of elements cannot be negative")
		}
	}

	d := Dict{}
	d.chunks[0] = make([]byte, 0, m)
	d.bitArr[0] = make([]uint64, 0, (m+63)>>6)
	// d.ranks[0] = make([]uint64, 0, (m+63)>>6) length? // Ranks wordt niet gebruikt door Read*List.
	// Moet ik ranks delen door 8 of 64??? Hoe hergebruiken bij Reset()???
	return &d, nil
}

// From constructs a dictionary from the given values.
// From automatically closes the dictionary for writing.
func From(values []uint64) *Dict {
	d := Dict{}
	d.chunks[0] = make([]byte, 0, len(values))
	d.bitArr[0] = make([]uint64, 0, (len(values)+63)>>6)
	d.WriteUint64List(values)
	d.Close()

	return &d
}

// Len returns the actual number of entries in the dictionary.
func Len(d *Dict) int {
	return len(d.chunks[0])
}

// Close builds support structures that improve the performance of direct reads.
// You should not do any direct reads before calling Close, as the read will
// crash. You can still write to the dictionary after a call to Close, but you
// will have to close the dictionary again before doing any direct reads.
func (d *Dict) Close() { // BuildIndex() noemen???
	for i := 0; i < nStreams64-1; i++ {
		arr := d.bitArr[i]
		if l := (len(arr) + 7) >> 3; len(d.ranks[i]) < l { // Moet ik delen door 8 of 64?
			d.ranks[i] = make([]int, 0, l)
		}

		var j, prefix int
		for j < len(arr) {
			if j&7 == 0 {
				d.ranks[i] = append(d.ranks[i], prefix)
			}
			prefix += bits.OnesCount64(arr[j])
			j++
		}
		d.ranks[i] = append(d.ranks[i], prefix)
	}
}

// Reset resets the dictionary without releasing its resources. It allows to
// re-use an existing dictionary.
func (d *Dict) Reset() {
	for i := range d.chunks {
		d.chunks[i] = d.chunks[i][:0]
	}

	for i := range d.bitArr {
		d.bitArr[i] = d.bitArr[i][:0]
	}
}

// WriteBool writes a boolean value to the dictionary.
func (d *Dict) WriteBool(v bool) int {
	if v {
		return d.WriteUint8(1)
	}
	return d.WriteUint8(0)
}

// WriteUint8 writes a uint8 value to the dictionary.
func (d *Dict) WriteUint8(v uint8) int {
	d.chunks[0] = append(d.chunks[0], v)
	d.extend(0)
	return 1
}

// WriteUint16 writes a uint16 value to the dictionary.
func (d *Dict) WriteUint16(v uint16) int {
	return d.WriteUint64(uint64(v))
}

// WriteUint32 writes a uint32 value to the dictionary.
func (d *Dict) WriteUint32(v uint32) int {
	return d.WriteUint64(uint64(v))
}

// WriteUint64 writes a uint64 value to the dictionary at index k.
func (d *Dict) Write(k int, v uint64) error { // Dit heeft zin? Maar je moet dan mogelijk wel heel wat d.chunks heralloceren, nl. wanneer het nieuwe getal strikt korter of langer is dan het oude! Dus duur!!!
	if k < 0 || len(d.chunks[0]) <= k {
		return errors.New("dac: key k is out of bounds")
	}

	n := (71 - bits.LeadingZeros64(v)) >> 3
	d.chunks[0][k] = uint8(v)

	for i := 1; i < n && i < nStreams64; i++ { // && k < len(d.chunks[i]) testen???
		v >>= 8
		d.chunks[i][k] = uint8(v)

		d.bitArr[i-1][k>>6] |= 1 << (k & 63)
	}

	return nil
}

// WriteUint64 writes a uint64 value to the dictionary.
func (d *Dict) WriteUint64(v uint64) int {
	n := (71 - bits.LeadingZeros64(v)) >> 3 // length (not right if v == 0)

	d.chunks[0] = append(d.chunks[0], uint8(v))
	d.extend(0)

	for i := 1; i < n && i < nStreams64; i++ {
		k := len(d.chunks[i-1]) - 1
		d.bitArr[i-1][k>>6] |= 1 << (k & 63)

		v >>= 8
		d.chunks[i] = append(d.chunks[i], uint8(v))
		d.extend(i)
	}

	return len(d.chunks[0]) - 1
}

// WriteUint643 writes a uint64 value to the dictionary.
func (d *Dict) WriteUint643(v uint64) int {
	n := (71 - bits.LeadingZeros64(v)) >> 3

	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	d.chunks[0] = append(d.chunks[0], buf[0])
	d.extend(0)

	for i := 1; i < n && i < nStreams64; i++ {
		k := len(d.chunks[i-1]) - 1
		d.bitArr[i-1][k>>6] |= 1 << (k & 63)

		d.chunks[i] = append(d.chunks[i], buf[i])
		d.extend(i)
	}

	return len(d.chunks[0]) - 1
}

// WriteUint64Unsafe writes a uint64 value to the dictionary.
func (d *Dict) WriteUint64Unsafe(v uint64) int {
	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	d.chunks[0] = append(d.chunks[0], buf[0])
	// d.extend(0)
	i := len(d.chunks[0]) - 1
	d.bitArr[0][i>>6] |= 1 << (i & 63)
	v >>= 8

	l := 1
	for v != 0 {
		d.chunks[l] = append(d.chunks[l], buf[l])
		// d.extend(int(l))
		i = len(d.chunks[l]) - 1
		d.bitArr[l][i>>6] |= 1 << (i & 63)
		v >>= 8
		l++
	}

	return len(d.chunks[0]) - 1
}

// WriteUint64Unsafe2 writes a uint64 value in reverse mode to the dictionary. = trager!!!
func (d *Dict) WriteUint64Unsafe2(v uint64) int {
	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	l := 1 + (bits.Len64(v>>8)+7)>>3

	for i := 0; i < l && i < nStreams64; i++ {
		d.chunks[i] = append(d.chunks[i], buf[i])
		// d.extend(i)
		k := len(d.chunks[i]) - 1
		d.bitArr[i][k>>6] |= 1 << (k & 63)
	}

	return len(d.chunks[0]) - 1
}

// WriteBoolList writes a slice of boolean values to the dictionary.
func (d *Dict) WriteBoolList(values []bool) {
	for _, v := range values {
		if v {
			d.chunks[0] = append(d.chunks[0], 1)
		} else {
			d.chunks[0] = append(d.chunks[0], 0)
		}
	}
	l := (len(d.chunks[0])+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)
}

// WriteUint8List writes a slice of uint8 values to the dictionary.
func (d *Dict) WriteUint8List(values []uint8) {
	d.chunks[0] = append(d.chunks[0], values...)
	l := (len(d.chunks[0])+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)
}

// WriteUint16List writes a slice of uint16 values to the dictionary.
func (d *Dict) WriteUint16List(values []uint16) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		n := (23 - bits.LeadingZeros16(v)) >> 3
		d.chunks[0] = append(d.chunks[0], uint8(v))

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			v >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(v))
			d.extend(i)
		}
	}
}

// WriteUint32List writes a slice of uint32 values to the dictionary.
func (d *Dict) WriteUint32List(values []uint32) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		n := (39 - bits.LeadingZeros32(v)) >> 3
		d.chunks[0] = append(d.chunks[0], uint8(v))

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			v >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(v))
			d.extend(i)
		}
	}
}

// WriteUint64List writes a slice of uint64 values to the dictionary.
func (d *Dict) WriteUint64List(values []uint64) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		n := (71 - bits.LeadingZeros64(v)) >> 3
		d.chunks[0] = append(d.chunks[0], uint8(v))

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			v >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(v))
			d.extend(i)
		}
	}
}

// WriteInt8 writes an int8 value to the dictionary.
func (d *Dict) WriteInt8(v int8) int {
	uv := uint8((v << 1) ^ (v >> 7))
	return d.WriteUint8(uv)
}

// WriteInt16 writes an int16 value to the dictionary.
func (d *Dict) WriteInt16(v int16) int {
	sv := int64(v)
	uv := uint64((sv << 1) ^ (sv >> 63))
	return d.WriteUint64(uv)
}

// WriteInt32 writes an int32 value to the dictionary.
func (d *Dict) WriteInt32(v int32) int {
	sv := int64(v)
	uv := uint64((sv << 1) ^ (sv >> 63))
	return d.WriteUint64(uv)
}

// WriteInt64 writes an int64 value to the dictionary.
func (d *Dict) WriteInt64(v int64) int {
	uv := uint64((v << 1) ^ (v >> 63))
	return d.WriteUint64(uv)
}

// WriteFloat32 writes a float32 value to the dictionary.
func (d *Dict) WriteFloat32(v float32) int {
	x := math.Float32bits(v)
	uv := uint64(bits.ReverseBytes32(x))
	// return d.WriteUint32(uv) // Ik denk dat d.WriteUint64(uv) sneller is.
	return d.WriteUint64(uv)
}

// WriteFloat64 writes a float64 value to the dictionary.
func (d *Dict) WriteFloat64(v float64) int {
	uv := bits.ReverseBytes64(math.Float64bits(v))
	return d.WriteUint64(uv)
}

// WriteDateTime writes a time.Time value with nanosecond
// precision to the dictionary. Timezones are not written.
func (d *Dict) WriteDateTime(t time.Time) int {
	return d.WriteInt64(t.UnixNano())
}

// WriteInt8List writes a slice of int8 values to the dictionary.
func (d *Dict) WriteInt8List(values []int8) {
	for _, v := range values {
		uv := uint8((v << 1) ^ (v >> 7))
		d.chunks[0] = append(d.chunks[0], uv)
	}
	l := (len(d.chunks[0])+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)
}

// WriteInt16List writes a slice of int16 values to the dictionary.
func (d *Dict) WriteInt16List(values []int16) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint16((v << 1) ^ (v >> 15))
		d.chunks[0] = append(d.chunks[0], uint8(uv))

		n := (23 - bits.LeadingZeros16(uv)) >> 3
		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// WriteInt32List writes a slice of int32 values to the dictionary.
func (d *Dict) WriteInt32List(values []int32) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint32((v << 1) ^ (v >> 31))
		d.chunks[0] = append(d.chunks[0], uint8(uv))

		n := (39 - bits.LeadingZeros32(uv)) >> 3

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// WriteInt64List writes a slice of int64 values to the dictionary.
func (d *Dict) WriteInt64List(values []int64) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint64((v << 1) ^ (v >> 63))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		n := (71 - bits.LeadingZeros64(uv)) >> 3

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// WriteFloat32List writes a slice of float values to the dictionary.
func (d *Dict) WriteFloat32List(values []float32) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := bits.ReverseBytes32(math.Float32bits(v))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		n := (39 - bits.LeadingZeros32(uv)) >> 3

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// WriteFloat64List writes a slice of float64 values to the dictionary.
func (d *Dict) WriteFloat64List(values []float64) {
	l := (len(d.chunks[0])+len(values)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := bits.ReverseBytes64(math.Float64bits(v))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		n := (71 - bits.LeadingZeros64(uv)) >> 3

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// WriteDateTimeList writes a slice of time.Time values to the dictionary.
func (d *Dict) WriteDateTimeList(dateTimes []time.Time) {
	l := (len(d.chunks[0])+len(dateTimes)+63)>>3 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, dt := range dateTimes {
		v := dt.UnixNano()
		uv := uint64((v << 1) ^ (v >> 63))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		n := (71 - bits.LeadingZeros64(uv)) >> 3

		for i := 1; i < n && i < nStreams64; i++ {
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			uv >>= 8
			d.chunks[i] = append(d.chunks[i], uint8(uv))
			d.extend(i)
		}
	}
}

// ReadBool reads a boolean value at a given index in the dictionary.
func (d *Dict) ReadBool(i int) (bool, error) {
	if i < 0 || Len(d) <= i {
		return false, errors.New("dac: key k is out of bounds")
	}
	return d.chunks[0][i] != 0, nil
}

// ReadUint8 reads an uint8 value at a given index in the dictionary.
func (d *Dict) ReadUint8(i int) (uint8, error) {
	if i < 0 || Len(d) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}
	return d.chunks[0][i], nil
}

// ReadUint16 reads an uint16 value at a given index in the dictionary.
func (d *Dict) ReadUint16(i int) (uint16, error) {
	v, err := d.ReadUint64(i)
	return uint16(v), err
}

// ReadUint32 reads an uint32 value at a given index in the dictionary.
func (d *Dict) ReadUint32(i int) (uint32, error) {
	v, err := d.ReadUint64(i)
	return uint32(v), err
}

// ReadUint64 reads an uint64 value at a given index in the dictionary.
func (d *Dict) ReadUint64(k int) (v uint64, err error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams64]byte)(unsafe.Pointer(&v)) // Waarschijnlijk is de buf op het einde sneller!
	buf[0] = d.chunks[0][k]

	var l int
	for l < nStreams64-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return
}

// ReadUint162 reads an uint64 value at a given index in the dictionary.
func (d *Dict) ReadUint162(k int) (uint16, error) { // Waarom traag???
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var buf [nStreams16]uint8
	buf[0] = d.chunks[0][k]
	if d.bit(0, k) {
		k = d.rank(0, k)
		buf[1] = d.chunks[1][k]
	}

	return *(*uint16)(unsafe.Pointer(&buf)), nil
}

// ReadUint322 reads an uint32 value at a given index in the dictionary.
func (d *Dict) ReadUint322(k int) (uint32, error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var l int
	var buf [nStreams32]uint8
	buf[0] = d.chunks[0][k]
	for l < nStreams32-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return *(*uint32)(unsafe.Pointer(&buf)), nil
}

// ReadUint642 reads an uint64 value at a given index in the dictionary.
func (d *Dict) ReadUint642(k int) (uint64, error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var buf [nStreams64]uint8
	buf[0] = d.chunks[0][k]

	var l int
	for l < nStreams64-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return *(*uint64)(unsafe.Pointer(&buf)), nil
}

// ReadBoolList returns all values in the dictionary when they are of boolean
// type. One can avoid the allocation of the return slice in ReadBoolList by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) ReadBoolList(values []bool) []bool {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]bool, m)
	} else {
		values = values[:m]
	}

	chunks := d.chunks[0]
	for i := 0; i < len(chunks) && i < len(values); i++ {
		if chunks[i] == 0 {
			values[i] = false
		} else {
			values[i] = true
		}
	}
	return values
}

// ReadUint8List returns all values in the dictionary when they are of uint8
// type. One can avoid the allocation of the return slice in ReadUint8List by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) ReadUint8List(values []uint8) []uint8 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint8, m)
	} else {
		values = values[:m]
	}

	for i := range values {
		values[i] = d.chunks[0][i]
	}
	return values
}

// ReadUint16List returns all values in the dictionary when they are of uint16
// type. One can avoid the allocation of the return slice in ReadUint16List by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) ReadUint16List(values []uint16) []uint16 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint16, m)
	} else {
		values = values[:m]
	}

	rank := -1
	for i := range values {
		buf := (*[nStreams16]byte)(unsafe.Pointer(&values[i]))
		buf[0] = d.chunks[0][i]
		if d.bit(0, i) {
			rank++
			k := rank
			buf[1] = d.chunks[1][k]
		}
	}
	return values
}

// ReadUint32List returns all values in the dictionary when they are of uint32
// type. One can avoid the allocation of the return slice in ReadUint32List by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) ReadUint32List(values []uint32) []uint32 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint32, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams32 - 1]int{-1, -1, -1}
	for i := range values {
		buf := (*[nStreams32]byte)(unsafe.Pointer(&values[i]))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams32-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
	}
	return values
}

// ReadList returns all values in the dictionary when they are of uint64
// type. One can avoid the allocation of the return slice in ReadList by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) ReadList(values []uint64) []uint64 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint64, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range values {
		buf := (*[nStreams64]byte)(unsafe.Pointer(&values[i]))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
	}
	return values
}

// Read64List returns all values in the dictionary when they are of uint64
// type. One can avoid the allocation of the return slice in Read64List by
// supplying a slice of a size sufficient to store all values. However,
// supplying a slice is optional.
func (d *Dict) Read64List(values []uint64) []uint64 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint64, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range values {
		buf := (*[nStreams64]byte)(unsafe.Pointer(&values[i]))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
	}
	return values
}

// ReadUint64List returns a slice of all elements in the dictionary. One can
// avoid the allocation of this slice in ReadList by supplying a slice of a
// size sufficient to store all values.
// func (d *Dict) ReadUint64List(values []uint64) []uint64 {
// 	m := len(d.chunks[0])
// 	if len(values) < m {
// 		values = make([]uint64, m)
// 	} else {
// 		values = values[:m]
// 	}

// 	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
// 	for i := range values {
// 		val := uint64(d.chunks[0][i])
// 		j, k := 0, i // j stream of level noemen!!!
// 		for j < nStreams64-1 && d.bit(j, k) {
// 			ranks[j]++
// 			k = ranks[j]
// 			j++
// 			val = val<<8 | uint64(d.chunks[j][k]) // Fout!!!
// 		}
// 		values[i] = val
// 	}
// 	return values
// }

// ReadInt8 reads an int8 value at a given index in the dictionary.
func (d *Dict) ReadInt8(i int) (int8, error) {
	uv, err := d.ReadUint8(i) // Kan nog sneller als ik ReadUint8 zelf inline.
	return int8((uv >> 1) ^ -(uv & 1)), err
}

// ReadInt16 reads an int16 value at a given index in the dictionary.
func (d *Dict) ReadInt16(i int) (int16, error) {
	uv, err := d.ReadUint64(i) // TODO: Is dit juist? Moeten we niet via uint16 gaan???
	return int16((uv >> 1) ^ -(uv & 1)), err
}

// ReadInt32 reads an int32 value at a given index in the dictionary.
func (d *Dict) ReadInt32(i int) (int32, error) {
	uv, err := d.ReadUint64(i)
	return int32((uv >> 1) ^ -(uv & 1)), err
}

// ReadInt64 reads an int64 value at a given index in the dictionary.
func (d *Dict) ReadInt64(i int) (int64, error) {
	uv, err := d.ReadUint64(i)
	return int64((uv >> 1) ^ -(uv & 1)), err
}

// // ReadInt643 reads an int64 at a given index in the dictionary.
// func (d *Dict) ReadInt643(i int) (int64, error) {
// 	uv, err := d.ReadUint64(i)
// 	return *(*int64)(unsafe.Pointer(&uv)), err
// }

// ReadFloat32 reads a float32 value at a given index in the dictionary.
func (d *Dict) ReadFloat32(i int) (float32, error) {
	if i < 0 || len(d.chunks[0]) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var uv uint32
	buf := (*[nStreams32]byte)(unsafe.Pointer(&uv)) // Waarschijnlijk is de buf op het einde sneller!
	buf[0] = d.chunks[0][i]

	l, k := 0, i
	for l < nStreams32-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}
	// fmt.Printf("%032b, %032b, %f\n", uv, bits.ReverseBytes32(uv), math.Float32frombits(bits.ReverseBytes32(uv)))
	return math.Float32frombits(bits.ReverseBytes32(uv)), nil
}

// ReadFloat322 reads a float32 value at a given index in the dictionary.
func (d *Dict) ReadFloat322(k int) (float32, error) {
	v, err := d.ReadUint32(k)
	return math.Float32frombits(bits.ReverseBytes32(v)), err
}

// ReadFloat64 reads a float64 value at a given index in the dictionary.
func (d *Dict) ReadFloat64(i int) (float64, error) {
	if i < 0 || len(d.chunks[0]) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var uv uint64
	buf := (*[nStreams64]byte)(unsafe.Pointer(&uv)) // Waarschijnlijk is de buf op het einde sneller!
	buf[0] = d.chunks[0][i]

	l, k := 0, i
	for l < nStreams64-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return math.Float64frombits(bits.ReverseBytes64(uv)), nil
}

// ReadFloat642 reads a float64 at a given index in the dictionary.
func (d *Dict) ReadFloat642(i int) (float64, error) {
	uv, err := d.ReadUint64(i)
	return math.Float64frombits(bits.ReverseBytes64(uv)), err
}

// ReadDateTime reads a time.Time value at a given index in the dictionary.
// No timezone is read.
func (d *Dict) ReadDateTime(i int) (time.Time, error) {
	v, err := d.ReadInt64(i)
	sec := v / 1e9
	nsec := v - 1e9*sec
	return time.Unix(sec, nsec), err
}

// ReadInt8List returns all values in the dictionary when they are of int8
// type. One can avoid the allocation of the return slice in ReadInt8List by
// supplying a slice of a size sufficient to store all values. However,
// this is optional.
func (d *Dict) ReadInt8List(values []int8) []int8 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]int8, m)
	} else {
		values = values[:m]
	}

	for i := range values {
		uv := d.chunks[0][i]
		values[i] = int8((uv >> 1) ^ -(uv & 1))
	}
	return values
}

// ReadInt16List returns all values in the dictionary when they are of int16
// type. One can avoid the allocation of the return slice in ReadInt16List by
// supplying a slice of a size sufficient to store all values. However,
// this is optional.
// func (d *Dict) ReadInt16List(values []int16) []int16 {
// 	m := len(d.chunks[0])
// 	if len(values) < m {
// 		values = make([]int16, m)
// 	} else {
// 		values = values[:m]
// 	}

// 	rank := -1
// 	for i := range values {
// 		var uv uint16
// 		buf := (*[nStreams16]byte)(unsafe.Pointer(&uv))
// 		buf[0] = d.chunks[0][i]

// 		if d.bit(0, i) {
// 			rank++
// 			buf[1] = d.chunks[1][rank]
// 		}
// 		values[i] = int16((uv >> 1) ^ -(uv & 1))
// 	}
// 	return values
// }

// ReadInt16List returns all values in the dictionary when they are of int16
// type. One can avoid the allocation of the return slice in ReadInt16List by
// supplying a slice of a size sufficient to store all values. However,
// this is optional.
func (d *Dict) ReadInt16List(values []int16) []int16 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]int16, m)
	} else {
		values = values[:m]
	}

	rank, chunks0, chunks1 := -1, d.chunks[0], d.chunks[1]
	for i := range values {
		var uv uint16
		buf := (*[nStreams16]byte)(unsafe.Pointer(&uv))
		buf[0] = chunks0[i]

		if d.bit(0, i) {
			rank++
			buf[1] = chunks1[rank]
		}
		values[i] = int16((uv >> 1) ^ -(uv & 1))
	}
	return values
}

// ReadInt32List returns all values in the dictionary when they are of int3é
// type. One can avoid the allocation of the return slice in ReadInt32List by
// supplying a slice of a size sufficient to store all values. However,
// this is optional.
func (d *Dict) ReadInt32List(values []int32) []int32 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]int32, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams32 - 1]int{-1, -1, -1}
	for i := range values {
		var uv uint32
		buf := (*[nStreams32]byte)(unsafe.Pointer(&uv))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams32-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
		values[i] = int32((uv >> 1) ^ -(uv & 1))
	}
	return values
}

// ReadInt64List returns all values in the dictionary when they are of int64
// type. One can avoid the allocation of the return slice in ReadInt64List by
// supplying a slice of a size sufficient to store all values. However,
// this is optional.
func (d *Dict) ReadInt64List(values []int64) []int64 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]int64, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range values {
		var uv uint64
		buf := (*[nStreams64]byte)(unsafe.Pointer(&uv))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
		values[i] = int64((uv >> 1) ^ -(uv & 1))
	}
	return values
}

// ReadFloat32List returns all values in the dictionary when they are of
// float32 type. One can avoid the allocation of the return slice in
// ReadFloat32List by supplying a slice of a size sufficient to store all
// values. However, this is optional.
func (d *Dict) ReadFloat32List(values []float32) []float32 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]float32, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams32 - 1]int{-1, -1, -1}
	for i := range values {
		var uv uint32
		buf := (*[nStreams32]byte)(unsafe.Pointer(&uv))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams32-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
		values[i] = math.Float32frombits(bits.ReverseBytes32(uv))
	}
	return values
}

// ReadFloat64List returns all values in the dictionary when they are of
// float64 type. One can avoid the allocation of the return slice in
// ReadFloat64List by supplying a slice of a size sufficient to store all
// values. However, this is optional.
func (d *Dict) ReadFloat64List(values []float64) []float64 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]float64, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range values {
		var uv uint64
		buf := (*[nStreams64]byte)(unsafe.Pointer(&uv))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
		values[i] = math.Float64frombits(bits.ReverseBytes64(uv))
	}
	return values
}

// ReadDateTimeList returns all values in the dictionary when they are of
// time.Time type. One can avoid the allocation of the return slice in
// ReadDateTimeList by supplying a slice of a size sufficient to store all
// values. However, this is optional.
func (d *Dict) ReadDateTimeList(dateTimes []time.Time) []time.Time {
	m := len(d.chunks[0])
	if len(dateTimes) < m {
		dateTimes = make([]time.Time, m)
	} else {
		dateTimes = dateTimes[:m]
	}

	ranks := [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range dateTimes {
		var uv uint64
		buf := (*[nStreams64]byte)(unsafe.Pointer(&uv))
		buf[0] = d.chunks[0][i]

		j, k := 0, i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
		v := int64((uv >> 1) ^ -(uv & 1))
		sec := v / 1e9
		nsec := v - 1e9*sec
		dateTimes[i] = time.Unix(sec, nsec)
	}
	return dateTimes
}

// Scan returns the index of the first instance of the search value in the
// dictionary. If the value is not found, -1 is returned. When the values
// are sorted, Search is going to be faster than Scan.
func (d *Dict) Scan(value uint64) (idx int) { // TODO: We zouden beter de high levels eerst scannen. Dan hebben we echter een Select() algoritme nodig!
	buf := (*[nStreams64]byte)(unsafe.Pointer(&value))
	n := maxByteIdx(value)

	search := buf[0]
	for i, v := range d.chunks[0] {
		if v == search {
			// search and v have length 1
			if n == 0 && !d.bit(0, i) {
				return i
			}

			// search and value are longer than 1 byte
			k, l := i, 0
			for l < n && d.bit(l, k) {
				k = d.rank(l, k)
				l++
				// not equal
				if buf[l] != d.chunks[l][k] {
					break
				}
				// equal and the same length
				if l == n && ((l < nStreams64-1 && !d.bit(l, k)) || (l == nStreams64-1)) {
					return i
				}
			}
		}
	}
	return -1
}

// Search returns the indexes in the dictionary of the searched value.
// If value is not found, an empty slice is returned. Search should
// only be used when the dictionary is sorted.
func (d *Dict) Search(value uint64) (idx, l int) {
	n := maxByteIdx(value)
	buf := (*[nStreams64]byte)(unsafe.Pointer(&value))

	// skip values of smaller size
	l = len(d.chunks[n])
	if n < nStreams64-1 {
		l -= len(d.chunks[n+1])
	}

	// search from upper stream downwards
	for {
		arr := d.chunks[n][idx : idx+l]
		lo := searchLo(arr, buf[n])
		if lo == l || arr[lo] != buf[n] {
			return -1, 0
		}
		idx += lo
		l = searchLen(arr[lo:], buf[n]) // TODO: variabele l wordt voor verschillende dingen gebruikt in verschillende functies!

		if n--; n < 0 {
			return
		}
		idx += len(d.chunks[n]) - len(d.chunks[n+1])
	}
}

// rank returns the rank of the (l+1)-th byte of the k-th number.
func (d *Dict) rank(l int, k int) int {
	blockID := k >> 9
	rank := d.ranks[l][blockID]

	start, end := blockID<<3, k>>6
	arr := d.bitArr[l]
	for i := start; i < end; i++ {
		rank += bits.OnesCount64(arr[i])
	}

	return rank + bits.OnesCount64(arr[end]&(1<<(k&63)-1))
}

// bit returns whether the bit in the given stream at the given position is set.
func (d *Dict) bit(stream, pos int) bool {
	return d.bitArr[stream][pos>>6]&(1<<(pos&63)) != 0
}

// extend extends the size of the bit array of a given stream.
func (d *Dict) extend(l int) {
	if len(d.chunks[l])&63 == 1 && l < nStreams64-1 {
		d.bitArr[l] = append(d.bitArr[l], 0)
	}
}

// searchLo returns the lowest index of value in arr.
func searchLo(arr []uint8, value uint8) int {
	lo, hi := 0, len(arr) // Test doen om lineair te scannen indien lengte kleiner dan threshold!

	for lo < hi {
		m := lo + (hi-lo)>>1
		if arr[m] < value {
			lo = m + 1
		} else {
			hi = m
		}
	}

	return lo
}

// searchLen returns the number of instances of value in arr.
func searchLen(arr []uint8, value uint8) int {
	lo, hi := 0, len(arr) // Test doen om lineair te scannen indien lengte kleiner dan threshold!

	for lo < hi {
		m := lo + (hi-lo)>>1
		if value < arr[m] {
			hi = m
		} else {
			lo = m + 1
		}
	}

	return hi
}

// maxByteIdx returns the index of the most significant byte in value.
func maxByteIdx(value uint64) int {
	n := (63 - bits.LeadingZeros64(value)) >> 3
	if n < 0 {
		return 0
	}
	return n
}
