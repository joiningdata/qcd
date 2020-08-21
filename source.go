package qcd

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/ulikunitz/xz"
)

// Source is a data source which has associated QCD checksum information.
type Source struct {
	// Filename contains the source data.
	Filename string

	// CheckFilename contains the QCD checksummer infomation.
	CheckFilename string

	ck *Checksummer

	lines []string
}

// NewSource creates a new QCD-verified data source.
func NewSource(filename string) (*Source, error) {
	checkfilename := filename
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	var src io.Reader = f

	if strings.HasSuffix(filename, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: filename looks like gzip but failed to open: %s\n", err.Error())
		} else {
			src = zr
		}
		checkfilename = strings.TrimSuffix(filename, ".gz")
	}
	if strings.HasSuffix(filename, ".bz2") {
		// FIXME: no way to detect errors until we read...
		src = bzip2.NewReader(f)
		checkfilename = strings.TrimSuffix(filename, ".bz2")
	}
	if strings.HasSuffix(filename, ".xz") {
		zr, err := xz.NewReader(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: filename looks like xz but failed to open: %s\n", err.Error())
		} else {
			src = zr
		}
		checkfilename = strings.TrimSuffix(filename, ".xz")
	}
	checkfilename += ".qcd"
	var vdata map[string]string

	vb, err := ioutil.ReadFile(checkfilename)
	if err == nil {
		err = json.Unmarshal(vb, &vdata)
	}
	if err != nil {
		return nil, err
	}

	ck := &Checksummer{}
	s := bufio.NewScanner(src)
	s.Buffer(make([]byte, maxLineLength), maxLineLength)
	val, numbad, err := ck.VerifyScanner(s, vdata)
	f.Close()
	if err != nil {
		return nil, err
	}
	if !val || numbad > 0 {
		return nil, fmt.Errorf("source failed self-verification")
	}

	data := make([]string, 0, ck.nrecs)
	f, err = os.Open(filename)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(filename, ".gz") {
		zr, _ := gzip.NewReader(f)
		src = zr
	}
	if strings.HasSuffix(filename, ".bz2") {
		// FIXME: no way to detect errors until we read...
		src = bzip2.NewReader(f)
	}
	if strings.HasSuffix(filename, ".xz") {
		zr, _ := xz.NewReader(f)
		src = zr
	}

	s = bufio.NewScanner(src)
	s.Buffer(make([]byte, maxLineLength), maxLineLength)
	for s.Scan() {
		record := s.Bytes()
		if ck.replacer != nil {
			record = ck.replacer.ReplaceAllLiteral(record, ck.replacement)
		}

		data = append(data, string(record))
	}
	f.Close()

	return &Source{
		CheckFilename: checkfilename,
		ck:            ck,
		Filename:      filename,
		lines:         data,
	}, nil
}

func (s *Source) DiffAgainst(other *Source, w io.Writer) bool {
	outLines := make(map[string]struct{})

	allmatch := true
	i, j := 0, 0
	for i < len(s.lines) && j < len(other.lines) {
		var rightHash [sha256.Size]byte

		if i == len(s.lines) {
			rightHash = sha256.Sum256([]byte(other.lines[j]))
			t := "+"
			if s.ck.recHashes.Has(rightHash[:]) {
				t = "*"
				if _, ok := outLines[other.lines[j]]; ok {
					j++
					continue
				}
			}

			fmt.Fprintln(w, t+other.lines[j])
			j++
			allmatch = false
			continue
		}

		leftHash := sha256.Sum256([]byte(s.lines[i]))
		rightHasLeft := other.ck.recHashes.Has(leftHash[:])
		if j == len(other.lines) {
			t := "-"
			if rightHasLeft {
				t = "*"
				if _, ok := outLines[s.lines[i]]; ok {
					i++
					continue
				}
			}

			fmt.Fprintln(w, t+s.lines[i])
			i++
			allmatch = false
			continue
		}
		rightHash = sha256.Sum256([]byte(other.lines[j]))
		leftHasRight := s.ck.recHashes.Has(rightHash[:])

		// easy match
		if s.lines[i] == other.lines[j] {
			outLines[s.lines[i]] = struct{}{}
			fmt.Fprintln(w, " "+s.lines[i])
			i++
			j++
			continue
		}

		/////
		// ok the standard diff would simply try to figure out
		// if something was added or removed from the left/right
		// ... but first we need to check if either was found
		//     somewhere on the opposite side out of order.

		if rightHasLeft && leftHasRight {
			// lines are out of order, keep the left-side ordering
			// and make sure that we output the right side at some point

			if _, ok := outLines[other.lines[j]]; ok {
				// right side was already output
				j++

				// by continuing, we have a chance for the left side
				// to exact-match the next line of the right side
				continue
			}

			if _, ok := outLines[s.lines[i]]; ok {
				// right side has never been output, but
				// left side was already output (maybe by the right side?)
				i++

				// by continuing, we have a chance for the right side
				// to exact-match the next line of the left side
				continue
			}

			// neither left or right has been output
			// we try to maintain left-ordering, so output that line now
			outLines[s.lines[i]] = struct{}{}
			fmt.Fprintln(w, "*"+s.lines[i])
			i++
			// since we know that the left has the right somewhere, it's safe to skip now
			j++
			continue
		}

		if rightHasLeft {
			// left side has no knowledge of the right, call it new
			outLines[other.lines[j]] = struct{}{}
			fmt.Fprintln(w, "+"+other.lines[j])
			j++
			allmatch = false
			continue
		}

		if leftHasRight {
			// right side has no knowledge of the left, call it removed
			outLines[s.lines[i]] = struct{}{}
			fmt.Fprintln(w, "-"+s.lines[i])
			i++
			allmatch = false
			continue
		}

		// if we made it here both sides are unknown to the other
		outLines[other.lines[j]] = struct{}{}
		outLines[s.lines[i]] = struct{}{}

		fmt.Fprintln(w, "-"+s.lines[i])
		fmt.Fprintln(w, "+"+other.lines[j])
		i++
		j++
		allmatch = false
	}
	return allmatch
}
