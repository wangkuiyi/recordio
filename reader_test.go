package recordio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexLocate(t *testing.T) {
	a := assert.New(t)

	idx := Index{accumChunkLens: []int{100, 200}}
	c, o := idx.Locate(0)
	a.Equal(0, c)
	a.Equal(0, o)

	c, o = idx.Locate(10)
	a.Equal(0, c)
	a.Equal(10, o)

	c, o = idx.Locate(100)
	a.Equal(1, c)
	a.Equal(0, o)

	c, o = idx.Locate(199)
	a.Equal(1, c)
	a.Equal(99, o)

	c, o = idx.Locate(200)
	a.Equal(-1, c)
	a.Equal(-1, o)
}
