package recordio

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAndRead(t *testing.T) {
	a := assert.New(t)
	const total = 2000
	writeAndReadRecords(a, total, NoCompression, true)
	writeAndReadRecords(a, total, NoCompression, false)
	writeAndReadRecords(a, total, Snappy, true)
	writeAndReadRecords(a, total, Snappy, false)
	writeAndReadRecords(a, total, Gzip, true)
	writeAndReadRecords(a, total, Gzip, false)
}

func writeAndReadRecords(a *assert.Assertions, total int, compressor int, incrementalLength bool) {
	var buf bytes.Buffer

	l := func(i int) int {
		if !incrementalLength {
			return total - i - 1
		}
		return i
	}

	w := NewWriter(&buf, -1, -1)
	for i := 0; i < total; i++ {
		_, e := w.Write(make([]byte, l(i))) // NOTE: the first/last record is empty.
		a.NoError(e)
	}
	a.NoError(w.Close())

	idx, e := LoadIndex(bytes.NewReader(buf.Bytes()))
	a.NoError(e)

	a.Equal(total, idx.NumRecords())

	s := NewScanner(bytes.NewReader(buf.Bytes()), idx, -1, -1)
	i := 0
	for s.Scan() {
		a.True(reflect.DeepEqual(s.Record(), make([]byte, l(i))))
		i++
	}
	a.Equal(total, i)
}

func TestWriteEmptyFile(t *testing.T) {
	assert := assert.New(t)

	var buf bytes.Buffer
	w := NewWriter(&buf, 10, NoCompression) // use a small maxChunkSize.
	assert.Nil(w.Close())
	assert.Equal(0, buf.Len())

	idx, e := LoadIndex(bytes.NewReader(buf.Bytes()))
	assert.Nil(e)
	assert.Equal(0, idx.NumRecords())
}

func BenchmarkRead(b *testing.B) {
	const (
		records   = 200
		rangeSize = 50
	)

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

	for s := 0; s < records-rangeSize; s += rangeSize {
		b.Run(fmt.Sprintf("reading records %05d to %05d", s, s+rangeSize),
			func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					scnr := NewScanner(f, idx, s, s+2*rangeSize)
					for scnr.Scan() {
						scnr.Record()
					}
				}
			})
	}
}

func synthesizeTempFile(records int) (fn string, e error) {
	f, e := ioutil.TempFile("", "benchmark-recordio")
	fn = f.Name()
	if e != nil {
		return "", e
	}

	w := NewWriter(bufio.NewWriter(f), 0, -1)
	rcd := make([]byte, 2*1024*1024)
	for i := 0; i < records; i++ {
		_, e = w.Write(rcd)
		if e != nil {
			return "", e
		}
	}
	w.Close()

	return fn, nil
}
