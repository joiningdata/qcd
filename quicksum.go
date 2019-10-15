package qcd

import (
	"bytes"
	"encoding/binary"
	"math"
)

// QuickSumSize classifications
type QuickSumSize byte

var (
	// DefaultSumSize for quicksum tracking
	// valid values are 'S', 'M', 'L', or '*'
	DefaultSumSize QuickSumSize = '*'
	// SmallSumSize is good for small data sets < 10k items
	SmallSumSize QuickSumSize = 'S'
	// MediumSumSize is good for medium data sets < 100k items
	MediumSumSize QuickSumSize = 'M'
	// LargeSumSize is good for large data sets > 100k items
	LargeSumSize QuickSumSize = 'L'
)

func newQuickSum(t QuickSumSize) quickSum {
	switch t {
	case SmallSumSize:
		return new(qc16)
	case MediumSumSize:
		return new(qc24)
	case LargeSumSize:
		return new(qc32)
	}
	// default
	return new(qcMeta)
}

type quickSum interface {
	Type() QuickSumSize
	Keys() int
	Bits() int
	Reset()
	Import([]byte) error
	Export() ([]byte, error)

	// always 32 bytes
	Add([]byte)
	// always 32 bytes
	Has([]byte) bool
}

/////////

// qcMeta can be used to auto-detect a good bloom filter size.
// if a smaller data structure is highly accurate, it will be
// preferred over a larger one.
type qcMeta struct {
	nadds int
	x16   qc16
	x24   qc24
	x32   qc32

	best quickSum
}

func (m *qcMeta) checkBest() {
	if m.best != nil {
		return
	}

	nkeys1 := float64(m.x16.Keys())
	bitsize1 := float64(m.x16.Bits())
	estError1 := math.Pow(1.0-math.Exp(-nkeys1*float64(m.nadds)/bitsize1), nkeys1)
	nkeys2 := float64(m.x24.Keys())
	bitsize2 := float64(m.x24.Bits())
	estError2 := math.Pow(1.0-math.Exp(-nkeys2*float64(m.nadds)/bitsize2), nkeys2)
	//nkeys3 := float64(m.x32.Keys())
	//bitsize3 := float64(m.x32.Bits())
	//estError3 := math.Pow(1.0-math.Exp(-nkeys3*float64(m.nadds)/bitsize3), nkeys3)

	if estError1 < 0.01 {
		m.best = &m.x16
		return
	}
	if estError2 < 0.01 {
		m.best = &m.x24
		return
	}
	if estError1 < 0.05 {
		m.best = &m.x16
		return
	}
	if estError2 < 0.1 {
		m.best = &m.x24
		return
	}

	if m.nadds > 500000 {
		m.best = &m.x32
		return
	}

	if estError1 < estError2 {
		m.best = &m.x16
		return
	}

	m.best = &m.x24
}

func (m *qcMeta) Type() QuickSumSize {
	m.checkBest()
	return m.best.Type()
}

func (m *qcMeta) Keys() int {
	m.checkBest()
	return m.best.Keys()
}

func (m *qcMeta) Bits() int {
	m.checkBest()
	return m.best.Bits()
}

func (m *qcMeta) Reset() {
	panic("can't reset a qcMeta!")
}

func (m *qcMeta) Import(v []byte) error {
	panic("can't import a qcMeta!")
}

func (m *qcMeta) Export() ([]byte, error) {
	m.checkBest()
	return m.best.Export()
}

func (m *qcMeta) Add(v []byte) {
	m.nadds++
	m.best = nil
	m.x16.Add(v)
	m.x24.Add(v)
	m.x32.Add(v)
}

func (m *qcMeta) Has(v []byte) bool {
	panic("can't has a qcMeta!")
}

/////////

// a 8 KByte bloom filter
//    with k=16 and 65,536 bits
//    uses a 2-byte window function across the sha256 hash
type qc16 [4096]uint16

func (x qc16) Type() QuickSumSize {
	return SmallSumSize
}

func (x qc16) Keys() int {
	return 16
}

func (x qc16) Bits() int {
	return 4096 * 16
}

func (x *qc16) Reset() {
	for i := range x {
		(*x)[i] = 0
	}
}

func (x *qc16) Import(v []byte) error {
	for i := 0; i < len(v); i += 2 {
		(*x)[i>>1] = uint16(v[i+1])<<8 | uint16(v[i])
	}
	return nil
}

func (x qc16) Export() ([]byte, error) {
	b := make([]byte, 4096*2)
	for i := range b {
		switch i & 1 {
		case 0:
			b[i] = byte(x[i>>1])
		case 1:
			b[i] = byte(x[i>>1] >> 8)
		}
	}
	return b, nil
}

func (x *qc16) Add(v []byte) {
	for i := 0; i < len(v)-2; i += 2 {
		idx := uint32(v[i])<<8 | uint32(v[i+1])
		idx >>= 4
		offs := uint16(v[i+1]) & 0x000F

		(*x)[idx] |= 1 << offs
	}
}

func (x qc16) Has(v []byte) bool {
	for i := 0; i < len(v)-2; i += 2 {
		idx := uint32(v[i])<<8 | uint32(v[i+1])
		idx >>= 4
		offs := uint16(v[i+1]) & 0x000F

		if (x[idx] & (1 << offs)) == 0 {
			return false
		}
	}
	return true
}

/////////

// a 2 MByte bloom filter
//    with k=10 and 16,777,217 bits
//    uses a 3-byte window function across the sha256 hash
type qc24 [1 << 20]uint16

func (x *qc24) Type() QuickSumSize {
	return MediumSumSize
}

func (x *qc24) Keys() int {
	return 10
}

func (x *qc24) Bits() int {
	return (1 << 20) * 16
}

func (x *qc24) Reset() {
	for i := range *x {
		(*x)[i] = 0
	}
}

func (x *qc24) Import(v []byte) error {
	bf := bytes.NewBuffer(v)
	return binary.Read(bf, binary.LittleEndian, x)
}

func (x *qc24) Export() ([]byte, error) {
	bf := &bytes.Buffer{}
	err := binary.Write(bf, binary.LittleEndian, *x)
	return bf.Bytes(), err
}

func (x *qc24) Add(v []byte) {
	for i := 0; i < len(v)-3; i += 3 {
		idx := uint32(v[i])<<16 | uint32(v[i+1])<<8 | uint32(v[i+2])
		idx >>= 4
		offs := uint16(v[i+2]) & 0x000F

		(*x)[idx] |= 1 << offs
	}
}

func (x *qc24) Has(v []byte) bool {
	for i := 0; i < len(v)-3; i += 3 {
		idx := uint32(v[i])<<16 | uint32(v[i+1])<<8 | uint32(v[i+2])
		idx >>= 4
		offs := uint16(v[i+2]) & 0x000F

		if (x[idx] & (1 << offs)) == 0 {
			return false
		}
	}
	return true
}

/////////

// a 512 MByte bloom filter
//    with k=8 and 4,294,967,296 bits
//    uses a 4-byte window function over the sha256 hash
type qc32 []uint32

func (x *qc32) Type() QuickSumSize {
	return LargeSumSize
}

func (x *qc32) Keys() int {
	return 8
}

func (x *qc32) Bits() int {
	return (1 << 27) * 32
}

func (x *qc32) Reset() {
	for i := range *x {
		(*x)[i] = 0
	}
}

func (x *qc32) Import(v []byte) error {
	*x = make([]uint32, 1<<27)
	bf := bytes.NewBuffer(v)
	return binary.Read(bf, binary.LittleEndian, x)
}

func (x *qc32) Export() ([]byte, error) {
	bf := &bytes.Buffer{}
	err := binary.Write(bf, binary.LittleEndian, *x)
	return bf.Bytes(), err
}

// when x is a sha256 sum (32 bytes)
//   this is similar to a bloom filter with k=8
//   and bitsize = 2^27 * 32
func (x *qc32) Add(v []byte) {
	if len(*x) == 0 {
		*x = make([]uint32, 1<<27)
	}
	for i := 0; i < len(v)-4; i += 4 {
		idx := (uint32(v[i]) << 24) | (uint32(v[i+1]) << 16) | (uint32(v[i+2]) << 8) | (uint32(v[i+3]))
		idx >>= 5
		offs := uint16(v[i+3]) & 0x001F

		(*x)[idx] |= uint32(1 << offs)
	}
}

func (x *qc32) Has(v []byte) bool {
	for i := 0; i < len(v)-4; i += 4 {
		idx := (uint32(v[i]) << 24) | (uint32(v[i+1]) << 16) | (uint32(v[i+2]) << 8) | (uint32(v[i+3]))
		idx >>= 5
		offs := uint16(v[i+3]) & 0x001F

		if ((*x)[idx] & uint32(1<<offs)) == 0 {
			return false
		}
	}
	return true
}
