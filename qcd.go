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
	"time"
)

// 1 MB
const maxLineLength = 1 << 20

// Checksummer can checksum the contents of a data stream
// independent of sort order. The zero-value is ready to use.
type Checksummer struct {
	sum         [sha256.Size]byte
	replacer    *regexp.Regexp
	replacement []byte

	recHashes quickSum
	nrecs     uint64

	vout io.Writer
}

// SetVerbose enables/disables verbose output.
func (c *Checksummer) SetVerbose(w io.Writer) {
	c.vout = w
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
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, maxLineLength), maxLineLength)
	return c.SumScanner(s)
}

// SumScanner scans records from the Scanner, applying any regex and
// replacement if defined, and adding the content to the checksum.
func (c *Checksummer) SumScanner(s *bufio.Scanner) error {
	if c.recHashes == nil {
		c.recHashes = newQuickSum(DefaultSumSize)
	}

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
	c.nrecs++
	c.recHashes.Add(nh[:])
	xorBytes(c.sum[:], c.sum[:], nh[:])
}

//////////////////

// Verify lines read from the provided io.Reader until EOF if hit.
func (c *Checksummer) Verify(r io.Reader, verify map[string]string) (bool, int, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, maxLineLength), maxLineLength)
	return c.VerifyScanner(s, verify)
}

// VerifyScanner scans records from the Scanner, applying any regex and
// replacement if defined, and verifying the content to the checksum.
func (c *Checksummer) VerifyScanner(s *bufio.Scanner, verify map[string]string) (bool, int, error) {
	err := c.unpackRecs(verify["records_hash"])
	if err != nil {
		return false, -1, err
	}

	if rx, ok := verify["mask_regex"]; ok && rx != "" {
		c.SetRegex(rx, verify["mask_replacement"])
	}

	nlines := 0
	noverify := 0
	for s.Scan() {
		nlines++
		if !c.verifyBytes(s.Bytes()) {
			noverify++
			if c.vout != nil {
				fmt.Fprintf(c.vout, "UNVERIFIED: %5d: %s\n", nlines, s.Text())
			}
		}
	}

	// check final content hash
	valid := verify["content_hash"] == fmt.Sprintf("%064x", c.sum)
	if valid {
		fmt.Fprintln(os.Stderr, "CHECKSUM OK")
		noverify = 0
	} else {
		fmt.Fprintln(os.Stderr, "CHECKSUM FAILED")
		fmt.Fprintf(os.Stderr, "%d/%d records failed verification\n", noverify, c.nrecs)
	}

	return valid, noverify, s.Err()
}

func (c *Checksummer) verifyBytes(record []byte) bool {
	if c.replacer != nil {
		record = c.replacer.ReplaceAllLiteral([]byte(record), c.replacement)
	}

	nh := sha256.Sum256(record)
	c.nrecs++
	b := c.recHashes.Has(nh[:])
	xorBytes(c.sum[:], c.sum[:], nh[:])
	return b
}

//////////////////

// Info returns a collection of statistics about the Checksums
// that were previously calculated:
//
//    "when_checked": UTC timestamp when the last checks were completed
//    "content_hash": a record-oriented uniqueness checksum (independent of ordering)
//    "records_hash": a hash of all the records observed that aids individual verification
//    "total_records": total count of records observed
//    "records_esterr": an estimated error rate for the record verifier
//    "mask_regex": regular rexpression used to identify and mask non-normative values
//    "mask_replacement": replacement text to use for masked values
//
func (c *Checksummer) Info() map[string]string {
	r := map[string]string{
		"when_checked":  time.Now().UTC().Format(time.RFC3339),
		"content_hash":  fmt.Sprintf("%064x", c.sum),
		"total_records": fmt.Sprint(c.nrecs),
	}
	if c.recHashes.Type() != DisableQuickSums {
		nkeys := float64(c.recHashes.Keys())
		bitsize := float64(c.recHashes.Bits())
		estError := math.Pow(1.0-math.Exp(-nkeys*float64(c.nrecs)/bitsize), nkeys)

		r["records_esterr"] = fmt.Sprint(estError)
		r["records_hash"] = c.packRecs()
	}
	if c.replacer != nil {
		r["mask_regex"] = c.replacer.String()
		r["mask_replacement"] = string(c.replacement)
	}
	return r
}

func (c *Checksummer) packRecs() string {
	if c.recHashes.Type() == DisableQuickSums {
		return ""
	}
	rhb, err := c.recHashes.Export()
	if err != nil {
		panic(err)
	}
	zb := &bytes.Buffer{}
	z, _ := gzip.NewWriterLevel(zb, gzip.BestSpeed)
	bb := [1]byte{byte(c.recHashes.Type())}
	z.Write(bb[:1])
	z.Write(rhb)
	z.Close()
	return base64.StdEncoding.EncodeToString(zb.Bytes())
}

func (c *Checksummer) unpackRecs(x string) error {
	if x == "" {
		c.recHashes = newQuickSum(DisableQuickSums)
		return nil
	}
	xb, err := base64.StdEncoding.DecodeString(x)
	if err != nil {
		return err
	}
	zb := bytes.NewBuffer(xb)
	z, _ := gzip.NewReader(zb)
	rhb, _ := ioutil.ReadAll(z)
	z.Close()
	c.recHashes = newQuickSum(QuickSumSize(rhb[0]))
	c.recHashes.Import(rhb[1:])
	return nil
}
