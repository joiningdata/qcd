package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
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
	var minTime time.Duration
	var elapsedTime time.Duration

	//time the method 3 times
	for i := 0; i < 3; i++ {
		//create the subprocess
		var process *exec.Cmd
		//if piping a sort, use bash shell
		if strings.Contains(method, "|") {
			process = exec.Command("bash", "-c", method)
		} else {
			//otherwise don't use a shell (and use io.Copy to pipe)
			args := strings.Split(method, " ")
			process = exec.Command(args[0], args[1:]...)
		}

		//only remove qcd if doing "before" timing
		if removeQCD && fileExists("qcd.txt") {
			os.Remove("qcd.txt")
		}

		//pipe from stdin
		stdin, err := process.StdinPipe()
		if err != nil {
			log.Fatal("Error getting pipe from stdin: ", err)
		}

		//write data.csv to stdin so process can read it
		file, err := os.Open("data.csv")
		if err != nil {
			log.Fatal("Can't open data.csv: ", err)
		}

		_, err = file.Stat()
		if err != nil {
			log.Fatal("Error getting file info: ", err)
		}

		//get output with bytes buffer
		var b bytes.Buffer
		process.Stdout = &b
		process.Stderr = &b

		//start timing
		start := time.Now()

		//start the process
		err = process.Start()
		if err != nil {
			log.Fatal("Error with Command: ", err)
		}

		//get input to command from data.csv file pipe (if not using bash shell)
		if !strings.Contains(method, "|") {
			_, err = io.Copy(stdin, file)
			if err != nil && err != io.EOF {
				// May be triggered by timeout
				fmt.Println(method)
				log.Fatal("Error writing from file to stdin pipe: ", err)
			}
		}

		//close file and stdin
		file.Close()
		stdin.Close()

		//wait for process to finish
		err = process.Wait()
		if err != nil {
			log.Fatal("Error finishing the process: ", err)
		}

		//get output
		// out := b.Bytes()
		// fmt.Println(string(out))

		//command finished, get the real time
		elapsedTime = time.Now().Sub(start)

		// if minTime is not set or is greater than elapsedTime
		if minTime == 0 || elapsedTime < minTime {
			minTime = elapsedTime
		}

	}
	fmt.Println("\t  MinTime: ", minTime, "\t(", method, ")")
	return strconv.FormatFloat(minTime.Seconds(), 'f', 6, 64)
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
		for rows <= 1000000 {
			//print # rows and cols
			fmt.Println("Computing... rows: ", rows, " cols: ", cols)

			//remove qcd.txt so the "before" test works
			if fileExists("qcd.txt") {
				os.Remove("qcd.txt")
			}

			//timing for writing data
			writeTime := time.Now()
			writeData(rows, cols)
			elapsedWriteTime := time.Now().Sub(writeTime)
			fmt.Println("\tTime to write data: ", elapsedWriteTime)
			//timing all checksums
			checksumTime := time.Now()
			times := []string{
				strconv.Itoa(rows),
				timeChecksum("./qcd -v qcd.txt", true),                         //qcd before (no qcd.txt should be found)
				timeChecksum("./qcd -v qcd.txt", false),                        //qcd after (checksum ok)
				timeChecksum("md5", false),                                     //md5 without the time it would take to pipe the sort
				timeChecksum("sort -k 1 -t , data.csv | md5", false),           //md5 with the time to pipe the sort
				timeChecksum("shasum -a 256", false),                           //sha without sort
				timeChecksum("sort -k 1 -t , data.csv | shasum -a 256", false), //sha with sort
			}
			outputwriter.Write(times)
			outputwriter.Flush()
			elapsedChecksum := time.Now().Sub(checksumTime)
			fmt.Println("\tTime for checksums: ", elapsedChecksum)
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
	elapsed := time.Now().Sub(start)
	fmt.Println("\nTotal time: ", elapsed)
}
