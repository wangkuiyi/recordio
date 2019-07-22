package recordio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAndReadHead(t *testing.T) {
	a := assert.New(t)

	c := &header{
		checkSum:       123,
		compressor:     456,
		compressedSize: 789,
	}

	var buf bytes.Buffer
	_, e := c.write(&buf)
	a.Nil(e)

	cc, e := parseHeader(&buf)
	a.Nil(e)
	a.Equal(c, cc)
}
