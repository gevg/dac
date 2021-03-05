// Package dac implements a compressed dictionary for booleans, datetimes,
// integers and floats of all sizes. Direct access to any value is obtained
// through the utilization of Directly Addressable Codes (DACs).
package dac

import (
	"errors"
	"math"
	"math/bits"
	"time"
	"unsafe"
)

const nStreams int = 8

// Dict is a dictionary type that stores values based on "Directly Addressable
// Codes". It allows to compactly store data while still providing direct
// access to any value. Data can be searched efficiently when stored in sorted
// order.
type Dict struct {
	bitArr [nStreams - 1][]uint64
	chunks [nStreams][]byte
}

// New constructs a dictionary with an initial capacity of n values. Setting
// the capacity is optional. It gets automatically expanded when needed.
func New(n ...int) (*Dict, error) {
	var m int
	if len(n) != 0 {
		if m = n[0]; m < 0 {
			return nil, errors.New("dac: number of elements cannot be a negative number")
		}
	}

	d := Dict{}
	d.chunks[0] = make([]byte, 0, m)
	d.bitArr[0] = make([]uint64, 0, (m+63)>>6)

	return &d, nil
}

// From constructs a dictionary from the given values.
// From automatically closes the dictionary for writing.
func From(values []uint64) *Dict {
	d := Dict{}
	d.chunks[0] = make([]byte, 0, len(values))
	d.bitArr[0] = make([]uint64, 0, (len(values)+63)>>6)
	d.WriteList(values)
	d.Close()

	return &d
}

// Close ... We moeten dan ook iets zeggen over Open(). Gebeurt steeds automatisch.
// Of bouwen we in dat je expliciet moet openen als de dict ook geclosed is geweest
// om te vergeten dat men fouten maakt (geen index gebouwd).
func (d *Dict) Close() { // Meermaals openen en closen kan geen kwaad.

}

// Reset resets the dictionary without releasing its resources.
func (d *Dict) Reset() {
	for i := range d.chunks {
		d.chunks[i] = d.chunks[i][:0]
	}

	for i := range d.bitArr {
		d.bitArr[i] = d.bitArr[i][:0]
	}
}

// Len returns the actual number of entries in the dictionary.
func Len(d *Dict) int {
	return len(d.chunks[0])
}

// WriteBool writes an boolean to the dictionary.
func (d *Dict) WriteBool(x bool) int {
	if x {
		return d.WriteUint64(1)
	}
	return d.WriteUint64(0)
}

// WriteDate writes a date to the dictionary.
func (d *Dict) WriteDate(t time.Time) int {
	return d.WriteInt64(t.Unix())
}

// WriteUint8 writes an uint8 to the dictionary.
func (d *Dict) WriteUint8(x uint8) int {
	return d.WriteUint64(uint64(x))
}

// WriteUint16 writes an uint16 to the dictionary.
func (d *Dict) WriteUint16(x uint16) int {
	return d.WriteUint64(uint64(x))
}

// WriteUint32 writes an uint32 to the dictionary.
func (d *Dict) WriteUint32(x uint32) int {
	return d.WriteUint64(uint64(x))
}

// WriteUint64 writes a value to the dictionary at index k.
func (d *Dict) Write(k int, x uint64) error { // Dit heeft zin? Maar je moet dan mogelijk wel heel wat d.chunks heralloceren, nl. wanneer het nieuwe getal strikt korter of langer is dan het oude! Dus duur!!!
	if k < 0 || len(d.chunks[0]) <= k {
		return errors.New("dac: key k is out of bounds")
	}

	n := (71 - bits.LeadingZeros64(x)) >> 3 // TODO!
	d.chunks[0][k] = uint8(x)

	for i := 1; i < n; i++ {
		x >>= 8
		d.chunks[i][k] = uint8(x)

		d.bitArr[i-1][k>>6] |= 1 << (k & 63)
	}

	return nil
}

// WriteUint64 writes an uint64 to the dictionary.
func (d *Dict) WriteUint64(x uint64) int {
	n := (71 - bits.LeadingZeros64(x)) >> 3

	d.chunks[0] = append(d.chunks[0], uint8(x))
	d.extend(0)

	for i := 1; i < n && i < len(d.chunks); i++ {
		k := len(d.chunks[i-1]) - 1
		d.bitArr[i-1][k>>6] |= 1 << (k & 63)

		x >>= 8
		d.chunks[i] = append(d.chunks[i], uint8(x))
		d.extend(i)
	}

	return len(d.chunks[0]) - 1
}

// WriteUint643 writes an uint64 to the dictionary.
func (d *Dict) WriteUint643(x uint64) int {
	n := (71 - bits.LeadingZeros64(x)) >> 3

	buf := (*[nStreams]byte)(unsafe.Pointer(&x))
	d.chunks[0] = append(d.chunks[0], buf[0])
	d.extend(0)

	for i := 1; i < n; i++ {
		k := len(d.chunks[i-1]) - 1 // TODO: Deze en volgende lijn in 1 functie steken???
		d.bitArr[i-1][k>>6] |= 1 << (k & 63)

		d.chunks[i] = append(d.chunks[i], buf[i])
		d.extend(i)
	}

	return len(d.chunks[0]) - 1
}

// WriteList writes a slice of uint64 elements to the dictionary.
func (d *Dict) WriteList(values []uint64) { // TODO: Does this call bring value as compared to just using WriteUint64() in a loop? Performance?
	for _, v := range values {
		n := (71 - bits.LeadingZeros64(v)) >> 3

		buf := (*[nStreams]byte)(unsafe.Pointer(&v)) // Unsafe or working with x>>8 instead?
		d.chunks[0] = append(d.chunks[0], buf[0])
		d.extend(0)

		for i := 1; i < n && i < nStreams; i++ { // boundscheck elimination: && i < nStreams  faster???
			k := len(d.chunks[i-1]) - 1
			d.bitArr[i-1][k>>6] |= 1 << (k & 63)

			d.chunks[i] = append(d.chunks[i], buf[i])
			d.extend(i)
		}
	}
}

// ReadUint64 reads an uint64 at a given index in the Dict dictionary.
func (d *Dict) ReadUint64(k int) (x uint64, err error) {
	if k < 0 || len(d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams]byte)(unsafe.Pointer(&x))
	buf[0] = d.chunks[0][k]

	var l int
	for l < nStreams-1 && d.bit(l, k) { // l < nStreams-1 && d.bitArr[l][k>>6]&(1<<(k&63)) != 0 {
		k = d.rank(l, k)
		l++
		buf[l] = d.chunks[l][k]
	}

	return
}

// ReadList returns a slice of all elements in the dictionary. One can avoid
// the allocation of this slice in ReadList by supplying a slice of a size
// sufficient to store all values.
func (d *Dict) ReadList(values []uint64) []uint64 {
	m := len(d.chunks[0])
	if len(values) < m {
		values = make([]uint64, m)
	} else {
		values = values[:m]
	}

	ranks := [nStreams - 1]int{-1, -1, -1, -1, -1, -1, -1}
	for i := range values {
		j := 0
		buf := (*[nStreams]byte)(unsafe.Pointer(&values[i]))
		buf[j] = d.chunks[j][i] // j stream noemen!!!
		for j < nStreams-1 && d.bit(j, i) {
			ranks[j]++
			i = ranks[j]
			j++
			buf[j] = d.chunks[j][i]
		}
	}
	return values
}

// bit returns whether the bit in the given stream at the given position is set.
func (d *Dict) bit(stream, pos int) bool {
	return d.bitArr[stream][pos>>6]&(1<<(pos&63)) != 0
}

// WriteUint64Unsafe writes a uint64 value to the dictionary.
func (d *Dict) WriteUint64Unsafe(x uint64) int {
	buf := (*[nStreams]byte)(unsafe.Pointer(&x))
	d.chunks[0] = append(d.chunks[0], buf[0])
	// d.extend(0)
	i := len(d.chunks[0]) - 1
	d.bitArr[0][i>>6] |= 1 << (i & 63)
	x >>= 8

	l := 1
	for x != 0 {
		d.chunks[l] = append(d.chunks[l], buf[l])
		// d.extend(int(l)) // Deze 3 lijnen moeten in een functie!!!
		i = len(d.chunks[l]) - 1
		d.bitArr[l][i>>6] |= 1 << (i & 63) // Dit moet in een functie van een vec met rank!!! bitArr[i] gaat over chunk[i-1]
		x >>= 8
		l++
	}

	return len(d.chunks[0]) - 1
}

// WriteUint64Unsafe2 writes an uint64 in reverse mode to the dictionary. = trager!!!
func (d *Dict) WriteUint64Unsafe2(x uint64) int {
	buf := (*[nStreams]byte)(unsafe.Pointer(&x))
	l := 1 + (bits.Len64(x>>8)+7)>>3

	for i := 0; i < l; i++ {
		d.chunks[i] = append(d.chunks[i], buf[i])
		// d.extend(i) // Deze 3 lijnen moeten in een functie!!!
		k := len(d.chunks[i]) - 1
		d.bitArr[i][k>>6] |= 1 << (k & 63) // Dit moet in een functie van een vec met rank!!! bitArr[i] gaat over chunk[i-1]
	}

	return len(d.chunks[0]) - 1
}

// WriteInt8 writes an int8 to the Dict dictionary.
func (d *Dict) WriteInt8(x int8) int {
	ux := uint8(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt16 writes an int16 to the Dict dictionary.
func (d *Dict) WriteInt16(x int16) int {
	ux := uint16(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt32 writes an int32 to the Dict dictionary.
func (d *Dict) WriteInt32(x int32) int {
	ux := uint32(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt64 writes an int64 to the Dict dictionary.
func (d *Dict) WriteInt64(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(ux)
}

// WriteFloat32 writes a float32 to the Dict dictionary.
func (d *Dict) WriteFloat32(x float32) int {
	return d.WriteUint32(math.Float32bits(x))
}

// WriteFloat64 writes a float64 to the Dict dictionary.
func (d *Dict) WriteFloat64(x float64) int {
	return d.WriteUint64(math.Float64bits(x))
}

// extend extends the size of the bit array of a given stream.
func (d *Dict) extend(l int) { // Volgens mij wordt de extend functie steeds maar duurder wanneer de arrays langer worden!! Oplossing???
	if len(d.chunks[l])&63 == 1 && l < nStreams-1 { // Als ik in de levels schrijf van n-1 naar 0, dan moet ik misschien extend(n-1) niet doen en kan ik de test l < nStreams-1 misschien wissen!
		d.bitArr[l] = append(d.bitArr[l], 0)
	}
}

// ReadBool reads a boolean at a given index in the Dict dictionary.
func (d *Dict) ReadBool(i int) (bool, error) {
	if Len(d) <= i {
		return false, errors.New("dac: key k is out of bounds")
	}
	return d.chunks[0][i]<<1 != 0, nil
}

// ReadDate reads a time at a given index in the Dict dictionary.
func (d *Dict) ReadDate(i int) (time.Time, error) {
	v, err := d.ReadInt64(i)
	return time.Unix(v, 0), err
}

// ReadUint8 reads an uint8 at a given index in the Dict dictionary.
func (d *Dict) ReadUint8(i int) (uint8, error) {
	v, err := d.ReadUint64(i)
	return uint8(v), err
}

// ReadUint16 reads an uint16 at a given index in the Dict dictionary.
func (d *Dict) ReadUint16(i int) (uint16, error) {
	v, err := d.ReadUint64(i)
	return uint16(v), err
}

// ReadUint32 reads an uint32 at a given index in the Dict dictionary.
func (d *Dict) ReadUint32(i int) (uint32, error) {
	v, err := d.ReadUint64(i)
	return uint32(v), err
}

// Waar kunnen we een range lezen??? Ik vermoed dat ik dit niet nodig zal hebben!

// ReadInt8 reads an int8 at a given index in the Dict dictionary.
func (d *Dict) ReadInt8(i int) (int8, error) {
	ux, err := d.ReadUint64(i)
	x := int8(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, err
}

// ReadInt16 reads an int16 at a given index in the Dict dictionary.
func (d *Dict) ReadInt16(i int) (int16, error) {
	ux, err := d.ReadUint64(i)
	x := int16(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, err
}

// ReadInt32 reads an int32 at a given index in the Dict dictionary.
func (d *Dict) ReadInt32(i int) (int32, error) {
	ux, err := d.ReadUint64(i)
	x := int32(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, err
}

// ReadInt64 reads an int64 at a given index in the Dict dictionary.
func (d *Dict) ReadInt64(i int) (int64, error) {
	ux, err := d.ReadUint64(i)
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, err
}

// ReadFloat32 reads a float32 at a given index in the Dict dictionary.
func (d *Dict) ReadFloat32(i int) (float32, error) {
	v, err := d.ReadUint32(i)
	return math.Float32frombits(v), err
}

// ReadFloat64 reads a float64 at a given index in the Dict dictionary.
func (d *Dict) ReadFloat64(i int) (float64, error) {
	v, err := d.ReadUint64(i)
	return math.Float64frombits(v), err
}

// bitRank is NOT an optimal implementation for long bit arrays. TODO: replace!!!
func (d *Dict) rank(l int, k int) int {
	var rank int

	for j := 0; j < k>>6; j++ {
		rank += bits.OnesCount64(d.bitArr[l][j])
	}

	return rank + bits.OnesCount64(d.bitArr[l][k>>6]&(1<<(k&63)-1))
}

// Search ...
func (d *Dict) Search(value uint64) (int, uint64) { // Of maken we hier NextGEQ van???
	// n := (71 - bits.LeadingZeros64(value)) >> 3
	// buf := (*[nStreams]byte)(unsafe.Pointer(&value))

	// for i := n - 1; i >= 0; i-- {

	// }

	// ranks := [nStreams - 1]int{-1, -1, -1, -1, -1, -1, -1}
	// for i := range values {
	// 	j := 0
	// 	buf := (*[nStreams]byte)(unsafe.Pointer(&values[i]))
	// 	buf[j] = d.chunks[j][i] // j stream noemen!!!
	// 	for j < nStreams-1 && d.bit(j, i) {
	// 		ranks[j]++
	// 		i = ranks[j]
	// 		j++
	// 		buf[j] = d.chunks[j][i]
	// 	}
	// }
	return 0, 0
}
