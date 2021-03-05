package dac

import "unsafe"

// Iterator ...
type Iterator struct {
	d     *Dict             // pointer to dictionary
	ranks [nStreams - 1]int // rank (starting to count from 0)
	k     int               // current index
}

// NewIterator ...
func NewIterator(d *Dict) Iterator {
	return Iterator{
		d:     d,
		ranks: [nStreams - 1]int{-1, -1, -1, -1, -1, -1, -1},
	}
}

// Value ...
func (it *Iterator) Value(k int) (v uint64, err error) { // TODO
	// i, j, k := it.k, 0, it.k
	// if ok = (Len(it.d) <= i); !ok {
	// 	return
	// }
	// it.k++

	// buf := (*[nStreams]byte)(unsafe.Pointer(&v))
	// buf[j] = it.d.chunks[j][i]
	// for j < nStreams-1 && it.d.bit(j, i) {
	// 	it.ranks[j]++
	// 	i = it.ranks[j]
	// 	j++
	// 	buf[j] = it.d.chunks[j][i]
	// }

	return
}

// Next ...
func (it *Iterator) Next() (k int, v uint64, ok bool) {
	i, j, k := it.k, 0, it.k
	if ok = (Len(it.d) <= i); !ok {
		return
	}
	it.k++

	buf := (*[nStreams]byte)(unsafe.Pointer(&v))
	buf[j] = it.d.chunks[j][i]
	for j < nStreams-1 && it.d.bit(j, i) {
		it.ranks[j]++
		i = it.ranks[j]
		j++
		buf[j] = it.d.chunks[j][i]
	}

	return
}

// p64 := it.pos[stream]
// b := it.buf[stream]
// for b == 0 {
// 	p64++
// 	b = it.d.bitArr[stream][p64]
// }
// b &= b - 1
// it.pos[stream] = p64
// it.buf[stream] = b & (b - 1)
// return p64<<6 + bits.TrailingZeros64(b)
// }

// func (it *Iterator) next(stream int) int {
// 	p64 := it.pos[stream]
// 	b := it.buf[stream]
// 	for b == 0 {
// 		p64++
// 		b = it.d.bitArr[stream][p64]
// 	}
// 	b &= b - 1
// 	it.pos[stream] = p64
// 	it.buf[stream] = b & (b - 1)
// 	return p64<<6 + bits.TrailingZeros64(b)
// }

// func (it *Iterator) next(stream int) (k int, v uint64) {
// 	k = it.k
// 	buf := (*[nStreams]byte)(unsafe.Pointer(&v))
// 	buf[0] = it.d.chunks[0][k]

// 	for i, b := range it.buf {
// 		p := it.pos[i]
// 		bitArr := it.d.bitArr[i]

// 		for b == 0 {
// 			p = p&^63 + 64
// 			b = bitArr[p>>6]
// 		}
// 		p = p&^63 + bits.TrailingZeros64(b)
// 		buf[i] = it.d.chunks[i+1][p]

// 		it.pos[i] = p
// 		it.buf[i] = b & (b - 1)
// 	}

// 	it.k++

// 	return
// }
