package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wangkuiyi/recordio"
)

func TestSyncReadOldFile(t *testing.T) {
	a := assert.New(t)
	f, e := os.Open("mnist/train/data-00000")
	a.NoError(e)

	idx, e := recordio.LoadIndex(f)
	a.NoError(e)

	a.Equal(16384, idx.NumRecords())

	s := recordio.NewScanner(f, idx, -1, -1)
	n := 0
	for s.Scan() {
		n++
	}
	a.Equal(16384, n)

	a.NoError(f.Close())
}

func TestAsyncReadOldFile(t *testing.T) {
	a := assert.New(t)

	fs, e := filepath.Glob("mnist/train/data-*")
	a.NoError(e)

	fl, e := recordio.NewFileList(fs)
	a.NoError(e)

	s := recordio.NewFileListScanner(fl, -1, -1)
	n := 0
	for range s.Chan() {
		n++
	}
	a.Equal(60000, n)
}
