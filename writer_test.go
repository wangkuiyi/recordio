package recordio

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSynthesizeAFile(t *testing.T) {
	a := assert.New(t)

	f, e := os.Create("/tmp/a_file.recordio")
	a.NoError(e)

	w := NewWriter(bufio.NewWriter(f), -1, -1)
	rcd := make([]byte, 2*1024)
	records := 100
	for i := 0; i < records; i++ {
		_, e = w.Write(rcd)
		a.NoError(e)
	}
	w.Close()
	f.Close()

	f, e = os.Open("/tmp/a_file.recordio")
	a.NoError(e)

	idx, e := LoadIndex(f)
	a.NoError(e)

	t.Logf("Index contains %d records", idx.NumRecords())
	a.NoError(f.Close())
}
