package recordio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexLocate(t *testing.T) {
	assert := assert.New(t)

	idx := Index{accumChunkLens: []int{100, 200}}
	c, o := idx.Locate(0)
	assert.Equal(0, c)
	assert.Equal(0, o)

	c, o = idx.Locate(10)
	assert.Equal(0, c)
	assert.Equal(10, o)

	c, o = idx.Locate(100)
	assert.Equal(1, c)
	assert.Equal(0, o)

	c, o = idx.Locate(199)
	assert.Equal(1, c)
	assert.Equal(99, o)

	c, o = idx.Locate(200)
	assert.Equal(-1, c)
	assert.Equal(-1, o)
}

func TestSyncReadOldFile(t *testing.T) {
	a := assert.New(t)
	f, e := os.Open("/Users/yi/Dropbox/data-00000")
	a.NoError(e)

	idx, e := LoadIndex(f)
	a.NoError(e)

	t.Logf("Index contains %d records", idx.NumRecords())
	t.Logf("Chunk sizes %v", idx.chunkRecords)

	s := NewScanner(f, idx, -1, -1)
	n := 0
	for s.Scan() {
		n++
	}
	t.Logf("Read %d records", n)

	a.NoError(f.Close())
}

func TestAsyncReadOldFile(t *testing.T) {
	a := assert.New(t)

	fl, e := NewFileList([]string{"/Users/yi/Dropbox/data-00000"})
	a.NoError(e)

	s := NewFileListScanner(fl, -1, -1)
	n := 0
	for range s.Chan() {
		n++
	}
	t.Logf("Read %d records", n)
}
