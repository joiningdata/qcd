package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/joiningdata/qcd"
)

func main() {
	//showVerbose := flag.Bool("e", false, "enable verbose errors")
	flag.Parse()

	fn1 := flag.Arg(0)
	fn2 := flag.Arg(1)
	if fn1 == "" || fn2 == "" {
		fmt.Fprintf(os.Stderr, "USAGE: %s base_file test_file\n", os.Args[0])
		os.Exit(-1)
	}
	left, err := qcd.NewSource(fn1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(-2)
	}
	right, err := qcd.NewSource(fn2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(-2)
	}

	///////////////////////

	left.DiffAgainst(right, os.Stdout)
}
