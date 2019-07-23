package recordio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteRead(t *testing.T) {
	const total = 2000
	var buf bytes.Buffer
	w := NewWriter(&buf, -1, -1)
	for i := 0; i < total; i++ {
		_, err := w.Write(make([]byte, i))
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	idx, err := LoadIndex(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if idx.NumRecords() != total {
		t.Fatal("num record does not match:", idx.NumRecords(), total)
	}

	s := NewScanner(bytes.NewReader(buf.Bytes()), idx, -1, -1)
	i := 0
	for s.Scan() {
		if !reflect.DeepEqual(s.Record(), make([]byte, i)) {
			t.Fatal("not equal:", len(s.Record()), len(make([]byte, i)))
		}
		i++
	}

	if i != total {
		t.Fatal("total count not match:", i, total)
	}
}

func TestWriteAndReadBigRecords(t *testing.T) {
	a := assert.New(t)

	f, e := ioutil.TempFile("", "recordio-test")
	a.NoError(e)
	defer os.Remove(f.Name())

	r := make([]byte, 4*1024*1024)
	for i := range r {
		r[i] = byte('A' + i%26)
	}

	w := NewWriter(f, -1, NoCompression)
	total := 10
	for i := 0; i < total; i++ {
		l, e := w.Write(r)
		a.Equal(len(r), l)
		a.NoError(e)
	}
	a.NoError(w.Close()) // closes f.

	f, e = os.Open(f.Name())
	a.NoError(e)
	idx, e := LoadIndex(f)
	a.NoError(e)
	a.NoError(f.Close())
	a.Equal(total, idx.NumRecords())

	f, e = os.Open(f.Name())
	a.NoError(e)
	scnr := NewScanner(f, idx, -1, -1)
	n := 0
	for scnr.Scan() {
		a.True(reflect.DeepEqual(r, scnr.Record()))
		n++
	}
	a.Equal(io.EOF, scnr.Error())
	a.Equal(total, n)
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
