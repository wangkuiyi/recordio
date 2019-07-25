package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/wangkuiyi/recordio"
)

func noErr(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func mustEqual(a, b int) {
	if a != b {
		log.Fatalf("Expecting equal values (%d vs %d)", a, b)
	}
}

func syncReadOldFile() {
	f, e := os.Open("mnist/train/data-00000")
	noErr(e)

	idx, e := recordio.LoadIndex(f)
	noErr(e)

	mustEqual(16384, idx.NumRecords())

	s := recordio.NewScanner(f, idx, 100, 256)
	n := 0
	for s.Scan() {
		n++
	}
	mustEqual(256, n)

	noErr(f.Close())
}

func asyncReadOldFile() {
	fs, e := filepath.Glob("mnist/train/data-*")
	noErr(e)

	fl, e := recordio.NewFileList(fs)
	noErr(e)

	s := recordio.NewFileListScanner(fl, -1, -1)
	n := 0
	for range s.Chan() {
		n++
	}
	mustEqual(60000, n)
}

func main() {
	syncReadOldFile()
	asyncReadOldFile()
	log.Printf("OK! Everything went good!")
}
