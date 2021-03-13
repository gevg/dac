package dac

import (
	"errors"
	"unsafe"
)

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
func (it *Iterator) Value(k int) (v uint64, err error) {
	if k < 0 || len(it.d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams]byte)(unsafe.Pointer(&v))
	buf[0] = it.d.chunks[0][k]

	var l int
	for l < nStreams-1 && it.d.bit(l, k) { // l < nStreams-1 && d.bitArr[l][k>>6]&(1<<(k&63)) != 0 {
		k = it.d.rank(l, k)
		it.ranks[l] = k
		l++
		buf[l] = it.d.chunks[l][k]
	}

	return
}

// Next ...
func (it *Iterator) Next() (k int, v uint64, ok bool) {
	i, j, k := it.k, 0, it.k
	if ok = (i < Len(it.d)); !ok {
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

// Reset ...
func (it *Iterator) Reset() {
	it.k = 0
	it.ranks = [nStreams - 1]int{-1, -1, -1, -1, -1, -1, -1}
}
