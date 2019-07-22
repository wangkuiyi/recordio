package recordio

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func synthesizeFiles(nfiles int) (dir string, files []string, e error) {
	dir, e = ioutil.TempDir("", "recordio-filelist-test")
	if e != nil {
		return "", nil, e
	}

	chunkSize := 10 * nfiles // The cap size of synthesized record.

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
			l, e := w.Write(r) // The first record in each file is empty.
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

	nfiles := 10
	dir, files, e := synthesizeFiles(nfiles)
	a.NoError(e)
	a.Equal(nfiles, len(files))
	defer os.RemoveAll(dir)

	fl, e := NewFileList(files)
	a.NoError(e)
	for i := range files {
		a.Equal(i, fl.indices[i].NumRecords())
	}
	a.Equal(nfiles*(nfiles-1)/2, fl.TotalRecords())
}
