package qcd

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"regexp"
	"unsafe"
)

// Checksummer can checksum the contents of a data stream
// independent of sort order. The zero-value is ready to use.
type Checksummer struct {
	sum         [sha256.Size]byte
	replacer    *regexp.Regexp
	replacement []byte

	// if a sha256 hash ~= [16]uint16
	// upper 12 bits used for index = (val>>4)
	// lower 4 bits used for bit offset = val & 0x000F
	recHashes [4096]uint16
	nrecs     uint64

	vout io.Writer
}

// SetRegex sets a regular expression that will be
// replaced for every input record.
func (c *Checksummer) SetRegex(regex, replacement string) (err error) {
	c.replacer, err = regexp.Compile(regex)
	if err == nil {
		c.replacement = []byte(replacement)
	}
	return
}

// Sum lines read from the provided io.Reader until EOF if hit.
func (c *Checksummer) Sum(r io.Reader) error {
	return c.SumScanner(bufio.NewScanner(r))
}

// SumScanner scans records from the Scanner, applying any regex and
// replacement if defined, and adding the content to the checksum.
func (c *Checksummer) SumScanner(s *bufio.Scanner) error {
	for s.Scan() {
		c.sumBytes(s.Bytes())
	}
	return s.Err()
}

func (c *Checksummer) sumBytes(record []byte) {
	if c.replacer != nil {
		record = c.replacer.ReplaceAllLiteral([]byte(record), c.replacement)
	}

	nh := sha256.Sum256(record)
	c.track(nh[:])
	xorBytes(c.sum[:], c.sum[:], nh[:])
}

//////////////////

// Verify lines read from the provided io.Reader until EOF if hit.
func (c *Checksummer) Verify(r io.Reader, verify map[string]string) (int, error) {
	return c.VerifyScanner(bufio.NewScanner(r), verify)
}

// VerifyScanner scans records from the Scanner, applying any regex and
// replacement if defined, and verifying the content to the checksum.
func (c *Checksummer) VerifyScanner(s *bufio.Scanner, verify map[string]string) (int, error) {
	c.vout = os.Stdout
	err := c.unpackRecs(verify["records_hash"])
	if err != nil {
		return -1, err
	}

	noverify := 0
	for s.Scan() {
		if !c.verifyBytes(s.Bytes()) {
			if c.vout != nil {
				fmt.Fprintln(c.vout, "UNVERIFIED: "+s.Text())
			} else {
				noverify++
			}
		}
	}
	return noverify, s.Err()
}

func (c *Checksummer) verifyBytes(record []byte) bool {
	if c.replacer != nil {
		record = c.replacer.ReplaceAllLiteral([]byte(record), c.replacement)
	}

	nh := sha256.Sum256(record)
	b := c.exists(nh[:])
	xorBytes(c.sum[:], c.sum[:], nh[:])
	return b
}

//////////////////

// when x is a sha256 sum (32 bytes)
//   this is similar to a bloom filter with k=16
//   and bitsize = 16*4096
func (c *Checksummer) track(x []byte) {
	c.nrecs++

	xw := *(*[]uint16)(unsafe.Pointer(&x))
	for i := 0; i < len(x); i += 2 {
		idx := xw[i/2] >> 4
		offs := xw[i/2] & 0x000F

		c.recHashes[idx] |= 1 << offs
	}
}

func (c *Checksummer) exists(x []byte) bool {
	xw := *(*[]uint16)(unsafe.Pointer(&x))
	for i := 0; i < len(x); i += 2 {
		idx := xw[i/2] >> 4
		offs := xw[i/2] & 0x000F

		if (c.recHashes[idx] & (1 << offs)) == 0 {
			return false
		}
	}
	return true
}

//////////////////

// Info returns a collection of statistics about the Checksums
// that were previously calculated.
//    "content_hash": a record-oriented uniqueness checksum (independent of ordering)
//    "error_estimate": an estimated error rate for the
//
func (c *Checksummer) Info() map[string]string {
	nkeys := 16.0
	bitsize := float64(len(c.recHashes) * 16)
	estError := math.Pow(1.0-math.Exp(-nkeys*float64(c.nrecs)/bitsize), nkeys)

	return map[string]string{
		"content_hash":   fmt.Sprintf("%064x", c.sum),
		"total_records":  fmt.Sprint(c.nrecs),
		"records_esterr": fmt.Sprint(estError),
		"records_hash":   c.packRecs(),
	}
}

func (c *Checksummer) packRecs() string {
	rhb := make([]byte, 0, 4096*2)
	for _, ux := range c.recHashes {
		rhb = append(rhb, byte(ux>>8), byte(ux))
	}
	zb := &bytes.Buffer{}
	z := gzip.NewWriter(zb)
	z.Write(rhb)
	z.Close()
	return base64.StdEncoding.EncodeToString(zb.Bytes())
}

func (c *Checksummer) unpackRecs(x string) error {
	xb, err := base64.StdEncoding.DecodeString(x)
	if err != nil {
		return err
	}
	zb := bytes.NewBuffer(xb)
	z, _ := gzip.NewReader(zb)
	rhb, _ := ioutil.ReadAll(z)
	z.Close()
	for ri := 0; ri < 4096*2; ri += 2 {
		c.recHashes[ri/2] = uint16(rhb[ri])<<8 | uint16(rhb[ri+1])
	}
	return nil
}
