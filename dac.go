// Package dac implements a dictionary for booleans, datetimes, integers and
// floats of all sizes. Fast access is obtained through the utilization of
// Directly Addressable Codes (DACs). Compression of the values is done with
// End-Tagged Dense Codes (ETDCs).
package dac // Zou beter dac of num heten!!! Kan ik de etdc importeren??? Wil ik wel etdc???

import (
	"math"
	"math/bits"
	"time"
	"unsafe"
)

// Boundschecks verwijderen door de belangrijkste routines in asm te herschrijven.
// We verliezen dan wel inlining. Als we het aantal getallen op voorhand kennen,
// kunnen we de 0-de bitcount berekenen en het geheugen voor die chunk voorzien.
// Voorzien we functies om alle elementen in 1 batch te schrijven? Of voor kleinere
// batches? Dit werkt zeker sneller! Maar de programmatie is misschien onhandig.
// Hoe zit het met performance verlies door teveel cachelines???

// Testen:
// 	Ik kan een getal dat geschreven is als uint32 lezen als uint64 zonder problemen.
// 	Kan ik een positieve int32 lezen als uint32 of uint64???

const numStr int = 8 // number of streams

// Dict ...
type Dict struct {
	bitArr  [numStr][]uint64 // bit-arrays
	chunks  [numStr][]byte   // chunk-arrays
	counter uint32           // counter for ID-generation
}

// New creates and returns a DAC dictionary.
// TODO: counter documenteren of wegdoen!
// TODO: optional parameter toevoegen die het
// aantal getallen voorstelt. Zodoende kunnen
// we de capaciteit van chunks[0] alloceren.
// We moeten dan wel een test voorzien in
// d.extend.
func New(counter uint32) Dict {
	return Dict{counter: counter - 1}
}

// New2 ...
func New2(num, counter uint32) Dict {
	d := Dict{counter: counter - 1}

	for i := 0; i < numStr-1; i++ {
		d.chunks[i] = make([]byte, 0, num)
		d.bitArr[i] = make([]uint64, (num+63)/64)

	}
	d.chunks[numStr-1] = make([]byte, 0, num)
	d.bitArr[numStr-1] = make([]uint64, (num+63)/64)

	return d
}

// Reset ...
func (d *Dict) Reset() {
	for i := 0; i < numStr; i++ {
		d.chunks[i] = d.chunks[i][:0]
		// d.bitArr[i] = d.bitArr[i][:0]
	}
	d.counter = 0
}

// WriteBool writes a boolean to the dictionary.
func (d *Dict) WriteBool(x bool) uint32 {
	if x {
		d.chunks[0] = append(d.chunks[0], 129)
		d.extend(0)
		d.counter++
		return d.counter
	}
	d.chunks[0] = append(d.chunks[0], 128)
	d.extend(0)
	d.counter++
	return d.counter
}

// WriteDate writes a date to the dictionary.
func (d *Dict) WriteDate(t time.Time) uint32 {
	return d.WriteInt64(t.Unix())
}

// WriteUint8 writes an uint8 to the dictionary.
func (d *Dict) WriteUint8(x uint8) uint32 {
	return d.WriteUint64(uint64(x))
}

// WriteUint16 writes an uint16 to the dictionary.
func (d *Dict) WriteUint16(x uint16) uint32 {
	return d.WriteUint64(uint64(x))
}

// WriteUint32 writes an uint32 to the dictionary.
func (d *Dict) WriteUint32(x uint32) uint32 {
	return d.WriteUint64(uint64(x))
}

// WriteUint64 writes an uint64 to the dictionary.
func (d *Dict) WriteUint64(x uint64) uint32 {
	d.chunks[0] = append(d.chunks[0], byte(x&127|128))
	d.extend(0)
	x >>= 7
	k := 1

	for x > 0 {
		x--
		d.chunks[k] = append(d.chunks[k], byte(x&127))
		d.extend(k)
		i := len(d.chunks[k-1]) - 1
		d.bitArr[k-1][i>>6] |= 1 << (i & 63)
		x >>= 7
		k++
	}

	d.counter++ // Waarvoor heb ik de counter nodig??? Ik kan de lengte ook kennen via len(d.chunks[0]).
	return d.counter
}

// WriteUint64r writes an uint64 in reverse mode to the dictionary.
func (d *Dict) WriteUint64r(x uint64) uint32 {
	// ETDC coding
	var buf [numStr]byte
	buf[0] = byte(x&127 | 128)
	x >>= 7
	l := 1

	for x > 0 {
		buf[l] = byte(x & 127)
		x >>= 7
		l++
	}

	// write to DAC in reverse mode
	var i int
	l--
	for i < l {
		d.chunks[i] = append(d.chunks[i], buf[l-i])
		d.extend(i) // Deze 3 lijnen moeten in een functie!!!
		j := len(d.chunks[i]) - 1
		d.bitArr[i][j>>6] |= 1 << (j & 63) // Dit moet in een functie van een vec met rank!!! bitArr[i] gaat over chunk[i-1]
		i++
	}
	d.chunks[l] = append(d.chunks[l], buf[0])
	d.extend(l)

	d.counter++
	return d.counter
}

// WriteUint64Unsafe writes a uint64 value to the dictionary.
func (d *Dict) WriteUint64Unsafe(x uint64) uint32 {
	buf := *(*[numStr]byte)(unsafe.Pointer(&x))
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

	d.counter++
	return d.counter
}

// WriteUint64Unsafe2 writes an uint64 in reverse mode to the dictionary. = trager!!!
func (d *Dict) WriteUint64Unsafe2(x uint64) uint32 {
	buf := *(*[numStr]byte)(unsafe.Pointer(&x))
	l := 1 + (bits.Len64(x>>8)+7)>>3

	for i := 0; i < l; i++ {
		d.chunks[i] = append(d.chunks[i], buf[i])
		// d.extend(i) // Deze 3 lijnen moeten in een functie!!!
		k := len(d.chunks[i]) - 1
		d.bitArr[i][k>>6] |= 1 << (k & 63) // Dit moet in een functie van een vec met rank!!! bitArr[i] gaat over chunk[i-1]
	}

	d.counter++
	return d.counter
}

// ReadUint64r reads an uint64 at a given index from the Dict dictionary.
func (d *Dict) ReadUint64r(i uint32) uint64 {
	// var x uint64

	// l, y := 0, uint64(d.chunks[0][i])
	// for y < 128 {
	// 	x = x<<7 + y
	// 	j := d.bitRank(l, i)

	// 	l++
	// 	y = uint64(d.chunks[l][j])
	// }

	// return base[l] + x<<7 + y - 128
	return 0
}

// 	// Als i groter is dan het aantal getallen in de dictionary, crasht de
// 	// functie bij het accessen van d.chunks[0][i]!
// 	buf[0] = d.chunks[0][i]
// 	// Als i groter is dan het aantal getallen in de dictionary, crasht de
// 	// functie bij het accessen van d.bitArr[l][i>>6]! Dit kan echter niet
// 	// gebeuren als we d.chunks[0][i] reeds gecheckt hebben!!!

// 	// Kunnen we hier eerst niet beter checken op de stopbit van ETCD vooraleer bitArr te gebruiken!!!
// 	// Dat kan als de bytes reversed worden weggeschreven!!!
// 	// for buf[l]&0x80 == 0 {
// 	for d.bitArr[l][i>>6]&(1<<(i&63)) != 0 {
// 		// De bitRank en d.chunks[l][i] in de loop zijn veilig!
// 		i = d.bitRank(l, i)
// 		l++
// 		buf[l] = d.chunks[l][i]
// 	}
// 	lm1 := l

// 	y := uint64(buf[l])
// 	for y < 128 {
// 		x = x<<7 + y
// 		l--
// 		y = uint64(buf[l]) // Wat als ik y = uint64(buf[l-1]) en daarna l--. Zou dit de boundschecking wegdoen?
// 	}
// 	return base[lm1] + x<<7 + y - 128
// }

// WriteInt8 writes an int8 to the Dict dictionary.
func (d *Dict) WriteInt8(x int8) uint32 {
	ux := uint8(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt16 writes an int16 to the Dict dictionary.
func (d *Dict) WriteInt16(x int16) uint32 {
	ux := uint16(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt32 writes an int32 to the Dict dictionary.
func (d *Dict) WriteInt32(x int32) uint32 {
	ux := uint32(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(uint64(ux))
}

// WriteInt64 writes an int64 to the Dict dictionary.
func (d *Dict) WriteInt64(x int64) uint32 {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return d.WriteUint64(ux)
}

// WriteFloat32 writes a float32 to the Dict dictionary.
func (d *Dict) WriteFloat32(x float32) uint32 {
	return d.WriteUint32(math.Float32bits(x))
}

// WriteFloat64 writes a float64 to the Dict dictionary.
func (d *Dict) WriteFloat64(x float64) uint32 {
	return d.WriteUint64(math.Float64bits(x))
}

// extend extends the size of the bit array of a given stream.
func (d *Dict) extend(l int) {
	if len(d.chunks[l])&63 == 1 {
		d.bitArr[l] = append(d.bitArr[l], 0)
	}
}

// ReadBool reads a boolean at a given index from the Dict dictionary.
func (d *Dict) ReadBool(i uint32) bool {
	if d.chunks[0][i]<<1 != 0 {
		return true
	}
	return false
}

// ReadDate reads a time at a given index from the Dict dictionary.
func (d *Dict) ReadDate(i uint32) time.Time {
	return time.Unix(d.ReadInt64(i), 0)
}

// ReadUint8 reads an uint8 at a given index from the Dict dictionary.
func (d *Dict) ReadUint8(i uint32) uint8 {
	return uint8(d.ReadUint64(i))
}

// ReadUint16 reads an uint16 at a given index from the Dict dictionary.
func (d *Dict) ReadUint16(i uint32) uint16 {
	return uint16(d.ReadUint64(i))
}

// ReadUint32 reads an uint32 at a given index from the Dict dictionary.
func (d *Dict) ReadUint32(i uint32) uint32 {
	return uint32(d.ReadUint64(i))
}

// Waar kunnen we een range lezen??? Ik vermoed dat ik dit niet nodig zal hebben!

// ReadUint64 reads an uint64 at a given index from the Dict dictionary.
func (d *Dict) ReadUint64(i uint32) (x uint64) { // In assembly implementeren?
	// var l int
	// var buf [numStr]byte

	// // Als i groter is dan het aantal getallen in de dictionary, crasht de
	// // functie bij het accessen van d.chunks[0][i]!
	// buf[0] = d.chunks[0][i]
	// // Als i groter is dan het aantal getallen in de dictionary, crasht de
	// // functie bij het accessen van d.bitArr[l][i>>6]! Dit kan echter niet
	// // gebeuren als we d.chunks[0][i] reeds gecheckt hebben!!!

	// // Kunnen we hier eerst niet beter checken op de stopbit van ETCD vooraleer bitArr te gebruiken!!!
	// // Dat kan als de bytes reversed worden weggeschreven!!!
	// // for buf[l]&0x80 == 0 {
	// for d.bitArr[l][i>>6]&(1<<(i&63)) != 0 {
	// 	// De bitRank en d.chunks[l][i] in de loop zijn veilig!
	// 	i = d.bitRank(l, i)
	// 	l++
	// 	buf[l] = d.chunks[l][i]
	// }
	// lm1 := l

	// y := uint64(buf[l])
	// for y < 128 {
	// 	x = x<<7 + y
	// 	l--
	// 	y = uint64(buf[l]) // Wat als ik y = uint64(buf[l-1]) en daarna l--. Zou dit de boundschecking wegdoen?
	// }
	// return base[lm1] + x<<7 + y - 128
	return 0
}

// ReadInt8 reads an int8 at a given index from the Dict dictionary.
func (d *Dict) ReadInt8(i uint32) int8 {
	ux := uint8(d.ReadUint64(i))
	x := int8(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

// ReadInt16 reads an int16 at a given index from the Dict dictionary.
func (d *Dict) ReadInt16(i uint32) int16 {
	ux := uint16(d.ReadUint64(i))
	x := int16(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

// ReadInt32 reads an int32 at a given index from the Dict dictionary.
func (d *Dict) ReadInt32(i uint32) int32 {
	ux := uint32(d.ReadUint64(i))
	x := int32(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

// ReadInt64 reads an int64 at a given index from the Dict dictionary.
func (d *Dict) ReadInt64(i uint32) int64 { // TODO: Wat als i groter is dan het aantal getallen in de dictionary???
	ux := d.ReadUint64(i)
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

// ReadFloat32 reads a float32 at a given index from the Dict dictionary.
func (d *Dict) ReadFloat32(i uint32) float32 {
	return math.Float32frombits(d.ReadUint32(i))
}

// ReadFloat64 reads a float64 at a given index from the Dict dictionary.
func (d *Dict) ReadFloat64(i uint32) float64 {
	return math.Float64frombits(d.ReadUint64(i))
}

// bitRank is NOT an optimal implementation for long bit arrays. TODO: replace!!!
// We hebben ondertussen een snelle rank implementatie!!!
func (d *Dict) bitRank(l int, i uint32) uint32 { // Kan je dit niet gewoon rank() noemen?
	var rank int

	for j := uint32(0); j < i>>6; j++ {
		rank += bits.OnesCount64(d.bitArr[l][j]) // Is het interessant om alle bitvectors in 1 array te steken???
	}

	// return uint32(rank + bits.OnesCount64(d.bitArr[l][i>>6]<<(63-i&63)))
	return uint32(rank + bits.OnesCount64(d.bitArr[l][i>>6]&(1<<(i&63)-1)))
}
