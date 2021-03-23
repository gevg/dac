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
	d.ranks[0] = make([]int, (m+63)>>9)

	return &d, nil
}

// From constructs a dictionary from the given values.
// From automatically closes the dictionary for writing.
func From(values []uint64) *Dict {
	d := Dict{}
	d.chunks[0] = make([]byte, 0, len(values))
	d.bitArr[0] = make([]uint64, 0, (len(values)+63)>>6)
	d.WriteU64List(values)
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
		if l := (len(arr) + 7) >> 3; len(d.ranks[i]) < l {
			d.ranks[i] = make([]int, l) // Is het niet sneller om te verlengen???
		}

		var prefix int
		for j, k := 0, 0; j < len(arr); j++ {
			if j&7 == 0 {
				d.ranks[i][k] = prefix
				k++
			}
			prefix += bits.OnesCount64(arr[j])
		}
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
		d.ranks[i] = d.ranks[i][:0]
	}
}

// WriteBool writes a boolean value to the dictionary.
func (d *Dict) WriteBool(v bool) int {
	if v {
		return d.WriteU8(1)
	}
	return d.WriteU8(0)
}

// WriteU8 writes a uint8 value to the dictionary.
func (d *Dict) WriteU8(v uint8) int {
	d.chunks[0] = append(d.chunks[0], v)
	d.extend(0)
	return len(d.chunks[0]) - 1
}

// WriteU16 writes a uint16 value to the dictionary.
func (d *Dict) WriteU16(v uint16) int {
	d.chunks[0] = append(d.chunks[0], uint8(v))
	v >>= 8
	d.extend(0)

	if v != 0 {
		k := len(d.chunks[0]) - 1
		d.bitArr[0][k>>6] |= 1 << (k & 63)

		d.chunks[1] = append(d.chunks[1], uint8(v))
		d.extend(1)
	}

	return len(d.chunks[0]) - 1
}

// WriteU32 writes a uint32 value to the dictionary.
func (d *Dict) WriteU32(v uint32) int {
	return d.WriteU64(uint64(v))
}

// // WriteU64At writes a uint64 value to index k of the dictionary.
// func (d *Dict) WriteU64At(k int, v uint64) error {
// 	if k < 0 || len(d.chunks[0]) <= k {
// 		return errors.New("dac: key k is out of bounds")
// 	}

// 	d.chunks[0][k] = uint8(v)
// 	v >>= 8

// 	l := uint(0)
// 	for d.bit(l, k) {
// 		if v != 0 { // TODO: Deze if wordt nooit genomen door onze tests!!!
// 			k = d.rank(l, k)
// 			d.chunks[l+1][k] = uint8(v)
// 			v >>= 8
// 		} else {
// 			kn := d.rank(l, k)
// 			d.bitArr[l][k>>6] &^= 1 << (k & 63)
// 			for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
// 				d.ranks[l][i] -= 1
// 			}
// 			k = kn
// 			d.chunks[l+1] = append(d.chunks[l+1][:k], d.chunks[l+1][k+1:]...)
// 		}
// 		l++
// 	}

// 	for v != 0 && l < nStreams64-1 {
// 		d.extendN(l, k) // Kan dit voor de start van de lus? Lijkt anders dubbel!
// 		d.bitArr[l][k>>6] |= 1 << (k & 63)

// 		for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
// 			d.ranks[l][i] += 1
// 		}
// 		k = d.rank(l, k) // Opgepast, de rank array kan te kort zijn!!!
// 		d.chunks[l+1] = append(d.chunks[l+1], 0)
// 		copy(d.chunks[l+1][k+1:], d.chunks[l+1][k:])
// 		d.chunks[l+1][k] = uint8(v)
// 		d.extendN(l+1, k) // Check dat bitArr[l][k>>6] zeker bestaat, anders nullen bijvoegen!

// 		v >>= 8
// 		l++
// 	}

// 	return nil
// }

// RemoveAt removes the entry at index k.
func (d *Dict) RemoveAt(k int) error {
	if k < 0 || len(d.chunks[0]) <= k {
		return errors.New("dac: key k is out of bounds")
	}

	d.chunks[0] = append(d.chunks[0][:k], d.chunks[0][k+1:]...)
	l := uint(0)

	for l < nStreams64-1 {
		if !d.bit(l, k) {
			d.removeIdx(l, k)
			break
		}
		kn := d.rank(l, k)
		d.removeIdx(l, k)
		k, l = kn, l+1
		d.chunks[l] = append(d.chunks[l][:k], d.chunks[l][k+1:]...)
	}

	return nil
}

// WriteU64At writes a uint64 value to index k of the dictionary.
func (d *Dict) WriteU64At(k int, v uint64) error { // TODO: Noem ik dit UpdateAt() en voorzien we ook een InsertAt()???
	if k < 0 || len(d.chunks[0]) <= k {
		return errors.New("dac: key k is out of bounds")
	}

	d.chunks[0][k] = uint8(v)
	v >>= 8

	l := uint(0)
	for l < nStreams64-1 && d.bit(l, k) { // Can I split the loop in 2 consecutive loops without an if statement in the loops?
		if v != 0 { // TODO: Deze if wordt nooit genomen door onze tests!!!
			k, l = d.rank(l, k), l+1
			d.chunks[l][k] = uint8(v)
			v >>= 8
		} else {
			kn := d.rank(l, k)
			d.updateRanks(l, k)
			d.bitArr[l][k>>6] &^= 1 << (k & 63)
			k, l = kn, l+1
			d.chunks[l] = append(d.chunks[l][:k], d.chunks[l][k+1:]...)
		}
	}

	for v != 0 && l < nStreams64-1 {
		d.insertIdx(l, k) // Eerst extendN doen

		d.extendN(l, k)                    // Kan dit voor de start van de lus? Lijkt anders dubbel!
		d.bitArr[l][k>>6] |= 1 << (k & 63) // TODO: Je moet die invoegen!!!

		for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
			d.ranks[l][i] += 1
		}
		k, l = d.rank(l, k), l+1 // Opgepast, de rank array kan te kort zijn!!!

		d.chunks[l] = append(d.chunks[l], 0)
		copy(d.chunks[l][k+1:], d.chunks[l][k:])
		d.chunks[l][k] = uint8(v)
		d.extendN(l, k) // Check dat bitArr[l][k>>6] zeker bestaat, anders nullen bijvoegen!

		v >>= 8
	}

	return nil
}

// insertIdx ...
func (d *Dict) insertIdx(l uint, k int) {
	d.extendIdx(l) // Zorg dat indexN steeds 1 bit leeg heeft zodat ik hier later in de functie niet moet op checken!

	// update ranks when a 1-bit is inserted at index (l,k)
	b := int(d.bitArr[l][k>>6] & (1 << (k & 63)))

	for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
		bb := int(d.bitArr[l][i<<3] & 1)
		d.ranks[l][i] += bb - b
		b = bb
	}

	// update ranks length // Nog nodig?
	// n := (len(d.chunks[l]) + 511) >> 9
	// d.ranks[l] = d.ranks[l][:n]

	// update bitArr when a 1-bit is inserted at index (l,k)
	k64 := k >> 6
	k63 := uint64(1) << (k & 63)
	mask := k63 - 1
	bt := d.bitArr[l][k64] >> 63
	d.bitArr[l][k64] = k63 | (d.bitArr[l][k64]<<1)&^mask | d.bitArr[l][k64]&mask

	for i := k64 + 1; i < len(d.bitArr[l]); i++ {
		tmp := d.bitArr[l][i] >> 63
		d.bitArr[l][i] = bt | d.bitArr[l][i]<<1
		bt = tmp
	}

	// update bitArr length
	// n = (len(d.chunks[l]) + 63) >> 6
	// d.bitArr[l] = d.bitArr[l][:n]
}

// removeIdx ...
func (d *Dict) removeIdx(l uint, k int) {
	// update ranks when bit at index (l,k) is removed
	// b := int(d.bitArr[l][k>>6] & (1 << (k & 63)))
	b := int(d.bitArr[l][k>>6] >> (k & 63) & 1) // Nog nakijken!
	for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
		bb := int(d.bitArr[l][i<<3] & 1)
		d.ranks[l][i] += bb - b
		b = bb
	}

	// update ranks length
	n := (len(d.chunks[l]) + 511) >> 9
	d.ranks[l] = d.ranks[l][:n]

	// remove bit at index (l,k) of bitArr and update bitArr[l]
	var bt uint64
	start := k >> 6
	for i := len(d.bitArr[l]) - 1; i >= start+1; i-- {
		tmp := d.bitArr[l][i] << 63
		d.bitArr[l][i] = bt | d.bitArr[l][i]>>1
		bt = tmp
	}
	mask := uint64(1<<(k&63) - 1)
	d.bitArr[l][start] = bt | (d.bitArr[l][start]>>1)&^mask | d.bitArr[l][start]&mask

	// update length of bitArr[l]
	n = (len(d.chunks[l]) + 63) >> 6
	d.bitArr[l] = d.bitArr[l][:n]
}

// updateRanks ...
func (d *Dict) updateRanks(l uint, k int) {
	b := int(d.bitArr[l][k>>6] >> (k & 63) & 1)
	for i := k>>9 + 1; i < len(d.ranks[l]); i++ {
		d.ranks[l][i] -= b
	}

	// update ranks length
	n := (len(d.chunks[l]) + 511) >> 9
	d.ranks[l] = d.ranks[l][:n]
}

// removeBit ...
// func (d *Dict) removeBit(l uint, k int) {
// 	var b uint64
// 	start := k >> 6
// 	for i := len(d.bitArr[l]) - 1; i >= start+1; i-- {
// 		tmp := d.bitArr[l][i] << 63
// 		d.bitArr[l][i] = b | d.bitArr[l][i]>>1
// 		b = tmp
// 	}
// 	mask := uint64(1<<(k&63) - 1)
// 	d.bitArr[l][start] = b | (d.bitArr[l][start]>>1)&^mask | d.bitArr[l][start]&mask

// 	n := (len(d.chunks[l]) + 63) >> 6
// 	d.bitArr[l] = d.bitArr[l][:n]
// }

// extendN ...
func (d *Dict) extendN(l uint, k int) {
	// Is k eigenlijk wel belangrijk? Moet ik niet gewoon kijken naar len(d.chunks[l])!!!
	if l < nStreams64-1 { // Is test nodig voor boundschecking. In principe mag je deze functie niet aanroepen met een foutieve l.
		n := (k+64)>>6 - len(d.bitArr[l])
		if n > 0 {
			d.bitArr[l] = append(d.bitArr[l], make([]uint64, n)...)
		}
		n = (k+512)>>9 + 1 - len(d.ranks[l])
		if n > 0 {
			d.ranks[l] = append(d.ranks[l], make([]int, n)...)
			if length := len(d.ranks[l]); length > n {
				for i := length - n - 1; i < length-1; i++ {
					d.ranks[l][i+1] = d.ranks[l][i]
				}
			}
		}
	}
}

// extendN2 ...
func (d *Dict) extendIdx(l uint) {
	lenChk := len(d.chunks[l])
	n := (lenChk+64)>>6 - len(d.bitArr[l])
	if n > 0 {
		d.bitArr[l] = append(d.bitArr[l], make([]uint64, n)...)
	}

	lenArr := len(d.bitArr[l])
	n = (lenArr+7)>>3 + 1 - len(d.ranks[l])
	if n > 0 {
		d.ranks[l] = append(d.ranks[l], make([]int, n)...)
		if length := len(d.ranks[l]); length > n {
			for i := length - n - 1; i < length-1; i++ {
				d.ranks[l][i+1] = d.ranks[l][i]
			}
		}
	}
}

// WriteU64 writes a uint64 value to the dictionary.
func (d *Dict) WriteU64(v uint64) int {
	d.chunks[0] = append(d.chunks[0], uint8(v))
	v >>= 8
	d.extend(0)

	for i := uint(0); v != 0 && i < nStreams64-1; i++ {
		k := len(d.chunks[i]) - 1
		d.bitArr[i][k>>6] |= 1 << (k & 63)

		d.chunks[i+1] = append(d.chunks[i+1], uint8(v))
		v >>= 8
		d.extend(i + 1)
	}

	return len(d.chunks[0]) - 1
}

// WriteBoolList writes a slice of boolean values to the dictionary.
func (d *Dict) WriteBoolList(values []bool) {
	l := (len(d.chunks[0])+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		if v {
			d.chunks[0] = append(d.chunks[0], 1)
		} else {
			d.chunks[0] = append(d.chunks[0], 0)
		}
	}
}

// WriteU8List writes a slice of uint8 values to the dictionary.
func (d *Dict) WriteU8List(values []uint8) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.chunks[0] = append(d.chunks[0], values...)
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)
}

// WriteU16List writes a slice of uint16 values to the dictionary.
func (d *Dict) WriteU16List(values []uint16) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		d.chunks[0] = append(d.chunks[0], uint8(v))
		v >>= 8

		if v != 0 {
			k := len(d.chunks[0]) - 1
			d.bitArr[0][k>>6] |= 1 << (k & 63)

			d.chunks[1] = append(d.chunks[1], uint8(v))
			d.extend(1)
		}
	}
}

// WriteU32List writes a slice of uint32 values to the dictionary.
func (d *Dict) WriteU32List(values []uint32) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		d.chunks[0] = append(d.chunks[0], uint8(v))
		v >>= 8

		for i := uint(0); v != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(v))
			v >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteU64List writes a slice of uint64 values to the dictionary.
func (d *Dict) WriteU64List(values []uint64) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		d.chunks[0] = append(d.chunks[0], uint8(v))
		v >>= 8

		for i := uint(0); v != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(v))
			v >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteI8 writes an int8 value to the dictionary.
func (d *Dict) WriteI8(v int8) int {
	uv := uint8((v << 1) ^ (v >> 7))
	return d.WriteU8(uv)
}

// WriteI16 writes an int16 value to the dictionary.
func (d *Dict) WriteI16(v int16) int {
	uv := uint16((v << 1) ^ (v >> 15))
	return d.WriteU16(uv)
}

// WriteI32 writes an int32 value to the dictionary.
func (d *Dict) WriteI32(v int32) int { // TODO: too slow!!!
	uv := uint32((v << 1) ^ (v >> 31))
	return d.WriteU32(uv)
}

// WriteI64 writes an int64 value to the dictionary.
func (d *Dict) WriteI64(v int64) int {
	uv := uint64((v << 1) ^ (v >> 63))
	return d.WriteU64(uv)
}

// WriteFloat32 writes a float32 value to the dictionary.
func (d *Dict) WriteFloat32(v float32) int {
	x := math.Float32bits(v)
	uv := uint64(bits.ReverseBytes32(x))
	// return d.WriteU32(uv) // Is d.WriteU64(uv) faster?
	return d.WriteU64(uv)
}

// WriteFloat64 writes a float64 value to the dictionary.
func (d *Dict) WriteFloat64(v float64) int {
	uv := bits.ReverseBytes64(math.Float64bits(v))
	return d.WriteU64(uv)
}

// WriteDateTime writes a time.Time value with nanosecond
// precision to the dictionary. Timezones are not written.
func (d *Dict) WriteDateTime(t time.Time) int {
	return d.WriteI64(t.UnixNano())
}

// WriteI8List writes a slice of int8 values to the dictionary.
func (d *Dict) WriteI8List(values []int8) {
	l := (len(d.chunks[0])+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint8((v << 1) ^ (v >> 7))
		d.chunks[0] = append(d.chunks[0], uv)
	}
}

// WriteI16List writes a slice of int16 values to the dictionary.
func (d *Dict) WriteI16List(values []int16) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint16((v << 1) ^ (v >> 15))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		if uv != 0 {
			k := len(d.chunks[0]) - 1
			d.bitArr[0][k>>6] |= 1 << (k & 63)

			d.chunks[1] = append(d.chunks[1], uint8(uv))
			d.extend(1)
		}
	}
}

// WriteI32List writes a slice of int32 values to the dictionary.
func (d *Dict) WriteI32List(values []int32) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint32((v << 1) ^ (v >> 31))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		for i := uint(0); uv != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(uv))
			uv >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteI64List writes a slice of int64 values to the dictionary.
func (d *Dict) WriteI64List(values []int64) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := uint64((v << 1) ^ (v >> 63))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		for i := uint(0); uv != 0 && i < nStreams64-1; i++ { // TODO: Deze lus wordt niet uitgevoerd door de testen!!!
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(uv))
			uv >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteFloat32List writes a slice of float values to the dictionary.
func (d *Dict) WriteFloat32List(values []float32) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := bits.ReverseBytes32(math.Float32bits(v))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		for i := uint(0); uv != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(uv))
			uv >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteFloat64List writes a slice of float64 values to the dictionary.
func (d *Dict) WriteFloat64List(values []float64) {
	l := (len(d.chunks[0])+len(values)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, v := range values {
		uv := bits.ReverseBytes64(math.Float64bits(v))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		for i := uint(0); uv != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(uv))
			uv >>= 8
			d.extend(i + 1)
		}
	}
}

// WriteDateTimeList writes a slice of time.Time values to the dictionary.
func (d *Dict) WriteDateTimeList(dateTimes []time.Time) {
	l := (len(d.chunks[0])+len(dateTimes)+63)>>6 - len(d.bitArr[0])
	d.bitArr[0] = append(d.bitArr[0], make([]uint64, l)...)

	for _, dt := range dateTimes {
		v := dt.UnixNano()
		uv := uint64((v << 1) ^ (v >> 63))
		d.chunks[0] = append(d.chunks[0], uint8(uv))
		uv >>= 8

		for i := uint(0); uv != 0 && i < nStreams64-1; i++ {
			k := len(d.chunks[i]) - 1
			d.bitArr[i][k>>6] |= 1 << (k & 63)

			d.chunks[i+1] = append(d.chunks[i+1], uint8(uv))
			uv >>= 8
			d.extend(i + 1)
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

// ReadU8 reads an uint8 value at a given index in the dictionary.
func (d *Dict) ReadU8(i int) (uint8, error) {
	if i < 0 || Len(d) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}
	return d.chunks[0][i], nil
}

// ReadU16 reads an uint16 value at a given index in the dictionary.
func (d *Dict) ReadU16(k int) (v uint16, err error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams16]byte)(unsafe.Pointer(&v))
	buf[0] = d.chunks[0][k]

	if d.bit(0, k) {
		k = d.rank(0, k)
		buf[1] = d.chunks[1][k]
	}

	return
}

// // ReadU32 reads an uint32 value at a given index in the dictionary.
// func (d *Dict) ReadU32(i int) (uint32, error) { // TODO: too slow!!!
// 	v, err := d.ReadU64(i)
// 	return uint32(v), err
// }

// ReadU32 reads an uint32 value at a given index in the dictionary.
func (d *Dict) ReadU32(k int) (v uint32, err error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams32]byte)(unsafe.Pointer(&v))
	buf[0] = d.chunks[0][k]

	if d.bit(0, k) {
		k = d.rank(0, k)
		buf[1] = d.chunks[1][k]
	} else {
		return
	}

	if d.bit(1, k) {
		k = d.rank(1, k)
		buf[2] = d.chunks[2][k]
	} else {
		return
	}

	if d.bit(2, k) {
		k = d.rank(2, k)
		buf[3] = d.chunks[3][k]
	}

	return
}

// ReadU64 reads an uint64 value at a given index in the dictionary.
func (d *Dict) ReadU64(k int) (v uint64, err error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	buf[0] = d.chunks[0][k]

	var l uint
	for l < nStreams64-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return
}

// ReadBoolList returns all values in the dictionary when they are of boolean
// type. One can avoid the allocation of the return slice in ReadBoolList by
// supplying a slice of a size sufficient to store all values. Still,
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

// ReadU8List returns all values in the dictionary when they are of uint8
// type. One can avoid the allocation of the return slice in ReadU8List by
// supplying a slice of a size sufficient to store all values. Still,
// supplying a slice is optional.
func (d *Dict) ReadU8List(values []uint8) []uint8 {
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

// ReadU16List returns all values in the dictionary when they are of uint16
// type. One can avoid the allocation of the return slice in ReadU16List by
// supplying a slice of a size sufficient to store all values. Still,
// supplying a slice is optional.
func (d *Dict) ReadU16List(values []uint16) []uint16 {
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
			buf[1] = d.chunks[1][rank]
		}
	}
	return values
}

// ReadU32List returns all values in the dictionary when they are of uint32
// type. One can avoid the allocation of the return slice in ReadU32List by
// supplying a slice of a size sufficient to store all values. Still,
// supplying a slice is optional.
func (d *Dict) ReadU32List(values []uint32) []uint32 {
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

		j, k := uint(0), i
		for j < nStreams32-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
	}
	return values
}

// ReadU64List returns all values in the dictionary when they are of uint64
// type. One can avoid the allocation of the return slice in ReadU64List by
// supplying a slice of a size sufficient to store all values. Still, supplying
// a slice is optional.
func (d *Dict) ReadU64List(values []uint64) []uint64 {
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

		j, k := uint(0), i
		for j < nStreams64-1 && d.bit(j, k) {
			ranks[j]++
			k = ranks[j]
			j++
			buf[j] = d.chunks[j][k]
		}
	}
	return values
}

// ReadI8 reads an int8 value at a given index in the dictionary.
func (d *Dict) ReadI8(i int) (int8, error) {
	uv, err := d.ReadU8(i)
	return int8((uv >> 1) ^ -(uv & 1)), err
}

// ReadI16 reads an int16 value at a given index in the dictionary.
func (d *Dict) ReadI16(i int) (int16, error) {
	uv, err := d.ReadU64(i) // TODO: Is dit juist? Moeten we niet via uint16 gaan???
	return int16((uv >> 1) ^ -(uv & 1)), err
}

// ReadI32 reads an int32 value at a given index in the dictionary.
func (d *Dict) ReadI32(i int) (int32, error) {
	uv, err := d.ReadU64(i)
	return int32((uv >> 1) ^ -(uv & 1)), err
}

// ReadI64 reads an int64 value at a given index in the dictionary.
func (d *Dict) ReadI64(i int) (int64, error) {
	uv, err := d.ReadU64(i)
	return int64((uv >> 1) ^ -(uv & 1)), err
}

// ReadFloat32 reads a float32 value at a given index in the dictionary.
func (d *Dict) ReadFloat32(i int) (float32, error) {
	if i < 0 || len(d.chunks[0]) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var uv uint32
	buf := (*[nStreams32]byte)(unsafe.Pointer(&uv))
	buf[0] = d.chunks[0][i]

	l, k := uint(0), i
	for l < nStreams32-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return math.Float32frombits(bits.ReverseBytes32(uv)), nil
}

// ReadFloat64 reads a float64 value at a given index in the dictionary.
func (d *Dict) ReadFloat64(i int) (float64, error) {
	if i < 0 || len(d.chunks[0]) <= i {
		return 0, errors.New("dac: key k is out of bounds")
	}

	var uv uint64
	buf := (*[nStreams64]byte)(unsafe.Pointer(&uv))
	buf[0] = d.chunks[0][i]

	l, k := uint(0), i
	for l < nStreams64-1 && d.bit(l, k) {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return math.Float64frombits(bits.ReverseBytes64(uv)), nil
}

// ReadDateTime reads a time.Time value at a given index in the dictionary.
// No timezone is read.
func (d *Dict) ReadDateTime(i int) (time.Time, error) {
	v, err := d.ReadI64(i)
	sec := v / 1e9
	nsec := v - 1e9*sec
	return time.Unix(sec, nsec), err
}

// ReadI8List returns all values in the dictionary when they are of int8
// type. One can avoid the allocation of the return slice in ReadI8List by
// supplying a slice of a size sufficient to store all values. Still,
// this is optional.
func (d *Dict) ReadI8List(values []int8) []int8 {
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

// ReadI16List returns all values in the dictionary when they are of int16
// type. One can avoid the allocation of the return slice in ReadI16List by
// supplying a slice of a size sufficient to store all values. Still,
// this is optional.
func (d *Dict) ReadI16List(values []int16) []int16 {
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

// ReadI32List returns all values in the dictionary when they are of int32
// type. One can avoid the allocation of the return slice in ReadI32List by
// supplying a slice of a size sufficient to store all values. Still,
// this is optional.
func (d *Dict) ReadI32List(values []int32) []int32 {
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

		j, k := uint(0), i
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

// ReadI64List returns all values in the dictionary when they are of int64
// type. One can avoid the allocation of the return slice in ReadI64List by
// supplying a slice of a size sufficient to store all values. Still,
// this is optional.
func (d *Dict) ReadI64List(values []int64) []int64 {
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

		j, k := uint(0), i
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

		j, k := uint(0), i
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

		j, k := uint(0), i
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

		j, k := uint(0), i
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
			k, l := i, uint(0)
			for l < uint(n) && d.bit(l, k) {
				k = d.rank(l, k)
				l++
				// not equal
				if buf[l] != d.chunks[l][k] {
					break
				}
				// equal and the same length
				if l == uint(n) && ((l < nStreams64-1 && !d.bit(l, k)) || (l == nStreams64-1)) {
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
func (d *Dict) rank(l uint, k int) int {
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
func (d *Dict) bit(stream uint, pos int) bool {
	return d.bitArr[stream][pos>>6]&(1<<(pos&63)) != 0
}

// extend extends the size of the bit array of a given stream.
func (d *Dict) extend(l uint) {
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
func maxByteIdx(value uint64) int { // TODO: Nog nodig?
	n := (63 - bits.LeadingZeros64(value)) >> 3
	if n < 0 {
		return 0
	}
	return n
}
