package recordio

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func synthesizeFiles() (dir string, files []string, e error) {
	nfiles := 10
	chunkSize := 10 * nfiles // The cap size of synthesized record.

	dir, e = ioutil.TempDir("", "recordio-filelist-test")
	if e != nil {
		return "", nil, e
	}

	for i := 0; i < nfiles; i++ {
		fn := path.Join(dir, fmt.Sprintf("%05d.recordio", i))
		files = append(files, fn)
		f, e := os.Create(fn)
		if e != nil {
			return "", nil, e
		}

		w := NewWriter(f, chunkSize, Snappy)
		for j := 0; j < i; j++ { // The i-th file contains i records.
			r := make([]byte, j*10)
			l, e := w.Write(r) // The first record since the second file is empty.
			if e != nil {
				return "", nil, e
			}
			if l != len(r) {
				return "", nil, fmt.Errorf("Writing %d byte, but did only %d", len(r), l)
			}
		}
		w.Close() // closes file automatically.
	}
	return dir, files, nil
}

func TestNewFileList(t *testing.T) {
	a := assert.New(t)

	dir, files, e := synthesizeFiles()
	a.NoError(e)
	defer os.RemoveAll(dir)

	fl, e := NewFileList(files)
	a.NoError(e)
	for i := range files {
		a.Equal(i, fl.indices[i].NumRecords())
	}
	nfiles := len(files)
	a.Equal(nfiles*(nfiles-1)/2, fl.TotalRecords())

	scnr := NewFileListScanner(fl, -1, -1)
	a.NoError(scnr.Error())

	n := 0
	for range scnr.Chan() {
		n++
	}
	a.Equal(nfiles*(nfiles-1)/2, n)
	a.NoError(scnr.Error())
}

func BenchmarkSyncAndAsyncRead(b *testing.B) {
	const records = 200

	fn, e := synthesizeTempFile(records)
	if e != nil {
		b.Fatalf("Cannot synthesize RecordIO file for benchmarking: %v", e)
	}
	defer os.Remove(fn)

	f, e := os.Open(fn)
	if e != nil {
		b.Fatalf("Cannot open synthesized file %s: %v", fn, e)
	}

	idx, e := LoadIndex(f)
	if e != nil {
		b.Fatalf("Failed indexing synthesized file %s: %v", fn, e)
	}

	workload := time.Duration(2) * time.Millisecond

	b.Run("Synch reading",
		func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scnr := NewScanner(f, idx, -1, -1)
				for scnr.Scan() {
					scnr.Record()
					time.Sleep(workload) // mimic workload
				}
			}
		})

	fl, e := NewFileList([]string{fn})
	if e != nil {
		b.Fatalf("NewFileList failed: %v", e)
	}

	b.Run("Async reading",
		func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				scnr := NewFileListScanner(fl, -1, -1)
				for range scnr.Chan() {
					time.Sleep(workload) // mimic workload
				}
			}
		})
}
