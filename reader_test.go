package recordio

import (
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
