package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"compress/bzip2"
	"compress/gzip"

	"github.com/ulikunitz/xz"

	"github.com/joiningdata/qcd"
)

func main() {
	showVerbose := flag.Bool("e", false, "enable verbose errors")
	rg := flag.String("r", "", "`regex` to mask unstable content (e.g. dates, offsets, etc.)")
	xrepl := flag.String("x", "", "`text` to use for masked content")
	vfile := flag.String("v", "%s.qcd", "verification data `filename` [%s replaced with input name]")
	zsize := flag.String("z", "*", "estimated data size (0, S, M, L)")
	flag.Parse()

	var src io.Reader = os.Stdin
	if fn := flag.Arg(0); fn != "" {
		f, err := os.Open(fn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening source file: %s\n", err.Error())
			os.Exit(-4)
		}
		defer f.Close()
		src = f

		if strings.HasSuffix(fn, ".gz") {
			zr, err := gzip.NewReader(f)
			if err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: filename looks like gzip but failed to open: %s\n", err.Error())
			} else {
				src = zr
			}
			fn = strings.TrimSuffix(fn, ".gz")
		}
		if strings.HasSuffix(fn, ".bz2") {
			// FIXME: no way to detect errors until we read...
			src = bzip2.NewReader(f)
			fn = strings.TrimSuffix(fn, ".bz2")
		}
		if strings.HasSuffix(fn, ".xz") {
			zr, err := xz.NewReader(f)
			if err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: filename looks like xz but failed to open: %s\n", err.Error())
			} else {
				src = zr
			}
			fn = strings.TrimSuffix(fn, ".xz")
		}

		if strings.Contains(*vfile, "%s") {
			if strings.HasPrefix(*vfile, "%s") {
				// full path replacement
				*vfile = fn + strings.TrimPrefix(*vfile, "%s")
			} else {
				bn := filepath.Base(fn)
				*vfile = fmt.Sprintf(*vfile, bn)
			}
		}
	} else {
		fmt.Fprintln(os.Stderr, "Reading from standard input...")
	}

	qcd.DefaultSumSize = qcd.QuickSumSize((*zsize)[0])

	doVerify := false
	var vdata map[string]string
	if *vfile != "" {
		doVerify = true
		vb, err := ioutil.ReadFile(*vfile)
		if err == nil {
			fmt.Fprintln(os.Stderr, "Reading verification data from", *vfile)
			err = json.Unmarshal(vb, &vdata)
		}
		if err != nil {
			if os.IsNotExist(err) {
				doVerify = false
			} else {
				fmt.Fprintf(os.Stderr, "Unable to verify: -v '%s'\n    %s", *vfile, err.Error())
				os.Exit(-3)
			}
		}
	}

	ck := &qcd.Checksummer{}
	if *rg != "" {
		err := ck.SetRegex(*rg, *xrepl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid Regex: -r '%s'\n    %s", *rg, err.Error())
			os.Exit(-2)
		}
	}
	if *showVerbose {
		ck.SetVerbose(os.Stderr)
	}

	if doVerify {
		ok, nb, err := ck.Verify(src, vdata)
		if err != nil {
			fmt.Fprintln(os.Stderr, "unable to verify", err)
			os.Exit(-3)
		}
		if !ok && nb == 0 {
			os.Exit(-1)
		}
		os.Exit(nb)
	}

	err := ck.Sum(src)
	if err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "an error occured: %s", err.Error())
		os.Exit(-4)
	}

	info := ck.Info()
	if *vfile != "" && !strings.Contains(*vfile, "%s") {
		f, err := os.Create(*vfile)
		if err == nil {
			fmt.Fprintln(os.Stderr, "Writing verification data to", *vfile)
			err = json.NewEncoder(f).Encode(info)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing verification file: %s", err.Error())
		}
		f.Close()
	}
	for key, val := range info {
		if len(val) > 100 {
			val = val[:50] + "..." + val[len(val)-50:]
		}
		fmt.Fprintf(os.Stderr, "%-20s: %s\n", key, val)
	}
}
