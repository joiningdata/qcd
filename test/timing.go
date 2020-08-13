package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// characters that can be found in the data.csv cells
const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"

// get truly random seed using current time
var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

// StringWithCharset creates a string of size <length> using the provided charset
func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// RandString returns a string of size <length> using the const charset
func RandString(length int) string {
	return StringWithCharset(length, charset)
}

// checks if a file exists
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// performs 3 rounds of timing the provided metho and returns the minimum recorded time in seconds
func timeChecksum(method string, removeQCD bool) string {
	//timeout after 10 seconds
	timeout, err := time.ParseDuration("10s")
	if err != nil {
		log.Fatal("Error parsing timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var minTime time.Duration
	var currentTime time.Duration

	fmt.Println(method)
	//time the method 3 times
	for i := 0; i < 3; i++ {
		//create the subprocess
		methodT := strings.Join([]string{"time ", method}, "")
		process := exec.CommandContext(ctx, "bash", "-c", methodT)

		//only remove qcd if doing "before" timing
		if removeQCD && fileExists("qcd.txt") {
			os.Remove("qcd.txt")
		}

		//pipe from stdin
		stdin, err := process.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}

		//write data.csv to stdin so process can read it
		file, err := os.Open("data.csv")
		if err != nil {
			log.Fatal("Can't open data.csv")
		}

		fileInfo, err := file.Stat()
		if err != nil {
			log.Fatal("Error getting file info")
		}
		fmt.Println(fileInfo.Size())
		// I don't know why, but for some reason this line is hanging on the second iteration (1000 rows)...
		// I originally implemented it using io.Copy (without byte limit)
		// but it was hanging, so I tried putting this limit in and its still hanging (even though file size > 100000 bytes)
		_, err = io.CopyN(stdin, file, 100000)
		if err != nil && err != io.EOF {
			log.Fatal("Error writing from file to stdin pipe: ", err)
		}
		fmt.Println("HERE!!!")
		file.Close()
		stdin.Close()

		//run the process and get the time
		out, err := process.CombinedOutput()
		if err != nil {
			log.Fatal(err)
		}

		//timeout exceeded
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Command timed out")
			currentTime = timeout
		}

		//command finished, get the real time
		// fmt.Println("method: ", method)
		// fmt.Println(string(out))

		//convert the output to a Duration
		re := regexp.MustCompile(`real\t(.*?)\n`)
		realtime := re.Find(out)[5:]
		re = regexp.MustCompile(`(.*?m)`)
		minutes := re.Find(realtime)
		minutes = minutes[:len(minutes)-1]
		re = regexp.MustCompile(`m(.*?)\.`)
		seconds := re.Find(realtime)
		seconds = seconds[1 : len(seconds)-1]
		re = regexp.MustCompile(`\.(.*?)s`)
		milliseconds := re.Find(realtime)
		milliseconds = milliseconds[1 : len(milliseconds)-1]

		//get duration of current time
		t := strings.Join([]string{string(minutes), "m", string(seconds), "s", string(milliseconds), "ms"}, "")
		currentTime, err = time.ParseDuration(string(t))
		if err != nil {
			log.Fatal("Error parsing time")
		}
		// if minTime is not set or is greater than currentTime
		if minTime == 0 || currentTime < minTime {
			minTime = currentTime
		}
	}
	return strconv.FormatFloat(minTime.Seconds(), 'f', 6, 32)
}

//open and write random data to data.csv given the number of rows and cols
func writeData(row int, col int) {
	if fileExists("data.csv") {
		os.Remove("data.csv")
	}
	file, err := os.Create("data.csv")
	if err != nil {
		log.Fatal("Error creating data.csv")
	}

	csvwriter := csv.NewWriter(file)

	//create data 2d array
	data := make([][]string, row+1)
	// for i := range data {
	// 	data[i] = make([]string, col)
	// }

	//create header row
	var cols []string
	for c := 0; c < col; c++ {
		cols = append(cols, strings.Join([]string{"col", strconv.Itoa(c)}, ""))
	}
	data[0] = cols

	for r := 0; r < row; r++ {
		var currentRow []string
		numMediumCols := 0
		//for each column, append a value to current row
		for c := 0; c < col; c++ {
			if c == 0 || (c == 1 && col > 5) { // add large text column
				//get rand number between 30 and 100
				r := seededRand.Intn(70) + 30
				//append random string to current row
				currentRow = append(currentRow, RandString(r))
			} else if c%4 == 0 && numMediumCols < 5 { // add medium text column
				//get rand number between 10 and 30
				r := seededRand.Intn(20) + 10
				//append random string to current row
				currentRow = append(currentRow, RandString(r))
			} else if c%3 == 0 { //add small text column
				//get rand number between 1 and 5
				r := seededRand.Intn(4) + 1
				//append random string to current row
				currentRow = append(currentRow, RandString(r))
			} else {
				//get rand number between 1 and 1000000
				r := seededRand.Intn(999999) + 1
				//append random number (as string) to current row
				currentRow = append(currentRow, strconv.Itoa(r))
			}
		}
		data[r+1] = currentRow
	}
	csvwriter.WriteAll(data)
	csvwriter.Flush()
}

func main() {
	file, err := os.Create("output.csv")
	if err != nil {
		log.Fatal("Error creating output.csv")
	}

	outputwriter := csv.NewWriter(file)

	//get start time
	start := time.Now()

	//for each set of columns 5,30,100
	for _, cols := range []int{5, 30, 100} {
		//output the # cols
		outputwriter.Write([]string{"COLS", strconv.Itoa(cols)})
		//write the header
		outputwriter.Write([]string{"ROWS", "QCD (before)", "QCD (after)", "MD5", "MD5 (sorted)", "SHA", "SHA (sorted)"})
		outputwriter.Flush()
		//for rows *10 until 1 million
		rows := 100
		for rows <= 10000 {
			//print # rows and cols
			fmt.Println("Computing... rows: ", rows, " cols: ", cols)

			//remove qcd.txt so the "before" test works
			if fileExists("qcd.txt") {
				os.Remove("qcd.txt")
			}

			//timing for writing data
			writeTime := time.Now()
			writeData(rows, cols)
			fmt.Println("\tTime to write data: ", time.Since(writeTime))
			//timing all checksums
			checksumTime := time.Now()
			times := []string{
				strconv.Itoa(rows),
				timeChecksum("./qcd -v qcd.txt", true),                         //qcd before (no qcd.txt should be found)
				timeChecksum("./qcd -v qcd.txt", false),                        //qcd after (checksum ok)
				timeChecksum("md5", false),                                     //md5 without time it would take to pipe the sort
				timeChecksum("sort -k 1 -t , data.csv | md5", false),           //md5 with time to pipe the sort
				timeChecksum("shasum -a 256", false),                           //sha without sort
				timeChecksum("sort -k 1 -t , data.csv | shasum -a 256", false), //sha with sort
			}
			outputwriter.Write(times)
			outputwriter.Flush()

			fmt.Println("\tTime for checksums: ", time.Since(checksumTime))
			fmt.Println("\t", times)
			rows = rows * 10
		}
		outputwriter.Write([]string{})
		outputwriter.Write([]string{})
		outputwriter.Flush()
	}
	fmt.Println("Done!")
	// os.Remove("data.csv")
	os.Remove("qcd.txt")
	fmt.Println("\nTotal time: ", time.Since(start))
}
