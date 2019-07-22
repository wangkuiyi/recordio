package recordio

import (
	"fmt"
	"io"
)

const (
	defaultMaxChunkSize = 32 * 1024 * 1024
)

// Writer creates a RecordIO file.
type Writer struct {
	io.Writer    // Set to nil to mark a closed writer.
	chunk        *chunk
	maxChunkSize int // total records size, excluding metadata, before compression.
	compressor   int
}

// NewWriter creates a RecordIO file writer.  Each chunk is compressed
// using the deflate algorithm given compression level.  Note that
// level 0 means no compression and -1 means default compression.
func NewWriter(w io.Writer, maxChunkSize, compressor int) *Writer {
	if maxChunkSize <= 0 {
		maxChunkSize = defaultMaxChunkSize
	}

	if compressor < 0 {
		compressor = defaultCompressor
	}

	return &Writer{
		Writer:       w,
		chunk:        &chunk{},
		maxChunkSize: maxChunkSize,
		compressor:   compressor}
}

// Writes a record.  It returns an error if Close has been called.
func (w *Writer) Write(record []byte) (int, error) {
	if w.Writer == nil {
		return 0, fmt.Errorf("Cannot write since writer had been closed")
	}

	if len(record) >= w.maxChunkSize {
		return 0, fmt.Errorf("Cannot write big record close to the chunk size")
	}

	if w.chunk.numBytes+len(record) > w.maxChunkSize {
		if e := w.chunk.write(w.Writer, w.compressor); e != nil {
			return 0, e
		}
	}

	w.chunk.add(record)
	return len(record), nil
}

// Close flushes the current chunk and makes the writer invalid.
func (w *Writer) Close() error {
	e := w.chunk.write(w.Writer, w.compressor)
	w.Writer = nil
	return e
}
