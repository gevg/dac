package dac

import (
	"errors"
	"unsafe"
)

// Iterator enables iteration over the dictionary. Concurrent iteration
// is allowed once the dictionary is closed (no more writing).
type Iterator struct {
	d     *Dict               // pointer to dictionary
	ranks [nStreams64 - 1]int // rank (starting to count from 0)
	k     int                 // current index
}

// NewIterator creates an iterator for the given dictionary.
// func NewIterator(d *Dict) Iterator { // TODO: What is the preferred way: NewIterator() or d.Iter()
// 	return Iterator{
// 		d:     d,
// 		ranks: [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1},
// 	}
// }

// Iter creates an iterator for the dictionary.
func (d *Dict) Iter() Iterator {
	return Iterator{
		d:     d,
		ranks: [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1},
	}
}

// Value returns the k-th value from the dictionary. It also sets the iterator
// state, so that subsequent calls to Next will return the k+1, k+2, ... value.
func (it *Iterator) Value(k int) (v uint64, err error) {
	if k < 0 || len(it.d.chunks[0]) <= k {
		return 0, errors.New("dac: key k is out of bounds")
	}

	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	buf[0] = it.d.chunks[0][k]

	var l uint
	for l < nStreams64-1 && it.d.bit(l, k) {
		k = it.d.rank(l, k)
		it.ranks[l] = k
		l++
		buf[l] = it.d.chunks[l][k]
	}

	return
}

// Next returns the next index and value from the dictionary.
// If there is not a next value, the ok return value will be false.
func (it *Iterator) Next() (k int, v uint64, ok bool) {
	i, k := it.k, it.k // Needs to be more transparent! and faster!
	if ok = (i < Len(it.d)); !ok {
		return
	}

	it.k++
	buf := (*[nStreams64]byte)(unsafe.Pointer(&v))
	buf[0] = it.d.chunks[0][i]

	var j uint
	for j < nStreams64-1 && it.d.bit(j, i) {
		it.ranks[j]++
		i = it.ranks[j]
		j++
		buf[j] = it.d.chunks[j][i]
	}

	return
}

// Reset resets the iterator, without releasing its resources. After Reset,
// the iterator points again to the first element of the dictionary.
func (it *Iterator) Reset() {
	it.k = 0
	it.ranks = [nStreams64 - 1]int{-1, -1, -1, -1, -1, -1, -1}
}
