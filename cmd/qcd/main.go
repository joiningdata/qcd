package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/joiningdata/qcd"
)

func main() {
	rg := flag.String("r", "", "`regex` to mask unstable content (e.g. dates, offsets, etc.)")
	xrepl := flag.String("x", "", "`text` to use for masked content")
	vfile := flag.String("v", "", "verification data `filename`")
	flag.Parse()

	doVerify := false
	var vdata map[string]string
	if *vfile != "" {
		doVerify = true
		vb, err := ioutil.ReadFile(*vfile)
		if err == nil {
			err = json.Unmarshal(vb, &vdata)
		}
		if err != nil {
			if os.IsNotExist(err) {
				doVerify = false
			} else {
				fmt.Fprintf(os.Stderr, "Unable to verify: -v '%s'\n    %s", *vfile, err.Error())
				os.Exit(3)
			}
		}
	}

	ck := &qcd.Checksummer{}
	if *rg != "" {
		err := ck.SetRegex(*rg, *xrepl)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid Regex: -r '%s'\n    %s", *rg, err.Error())
			os.Exit(1)
		}
	}

	if doVerify {
		nb, err := ck.Verify(os.Stdin, vdata)
		if nb>0 || err!=nil {
			fmt.Fprintln(os.Stderr, "verification failed", nb, err)
			os.Exit(7)
		}
		os.Exit(0)
	}

	err := ck.Sum(os.Stdin)
	if err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "an error occured: %s", err.Error())
		os.Exit(2)
	}

	info := ck.Info()
	if *vfile!="" {
		f, err := os.Create(*vfile)
		if err==nil {
			err = json.NewEncoder(f).Encode(info)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing verification file: %s", err.Error())
		}
		f.Close()
	}
	for key, val := range info {
		fmt.Fprintf(os.Stderr, "%-20s: %s\n", key, val)
	}
}
