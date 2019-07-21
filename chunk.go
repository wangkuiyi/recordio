package recordio

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
)

// A chunk contains the Header and optionally compressed records.  To
// create a chunk, just use ch := &chunk{}.
type chunk struct {
	records  [][]byte
	numBytes int // sum of record lengths.
}

func (ch *chunk) add(record []byte) {
	ch.records = append(ch.records, record)
	ch.numBytes += len(record)
}

// dump the chunk into w, and clears the chunk and makes it ready for
// the next add invocation.
func (ch *chunk) dump(w io.Writer, compressorIndex int) error {
	// NOTE: don't check ch.numBytes instead, because empty
	// records are allowed.
	if len(ch.records) == 0 {
		return nil
	}

	// Write raw records and their lengths into data buffer.
	var data bytes.Buffer

	for _, r := range ch.records {
		var rs [4]byte
		binary.LittleEndian.PutUint32(rs[:], uint32(len(r)))

		if _, e := data.Write(rs[:]); e != nil {
			return fmt.Errorf("Failed to write record length: %v", e)
		}

		if _, e := data.Write(r); e != nil {
			return fmt.Errorf("Failed to write record: %v", e)
		}
	}

	compressed, e := compressData(&data, compressorIndex)
	if e != nil {
		return e
	}

	// Write chunk header and compressed data.
	hdr := &header{
		checkSum:       crc32.ChecksumIEEE(compressed.Bytes()),
		compressor:     uint32(compressorIndex),
		compressedSize: uint32(compressed.Len()),
		numRecords:     uint32(len(ch.records)),
	}

	if _, e := hdr.write(w); e != nil {
		return fmt.Errorf("Failed to write chunk header: %v", e)
	}

	if _, e := w.Write(compressed.Bytes()); e != nil {
		return fmt.Errorf("Failed to write chunk data: %v", e)
	}

	// Clear the current chunk.
	ch.records = nil
	ch.numBytes = 0

	return nil
}

type noopCompressor struct {
	*bytes.Buffer
}

func (c *noopCompressor) Close() error {
	return nil
}

func compressData(src io.Reader, compressorIndex int) (*bytes.Buffer, error) {
	compressed := new(bytes.Buffer)
	var compressor io.WriteCloser

	switch compressorIndex {
	case NoCompression:
		compressor = &noopCompressor{compressed}
	case Snappy:
		compressor = snappy.NewBufferedWriter(compressed)
	case Gzip:
		compressor = gzip.NewWriter(compressed)
	default:
		return nil, fmt.Errorf("Unknown compression algorithm: %d", compressorIndex)
	}

	if _, e := io.Copy(compressor, src); e != nil {
		return nil, fmt.Errorf("Failed to compress chunk data: %v", e)
	}
	compressor.Close()

	return compressed, nil
}

// parse the specified chunk from r.
func parseChunk(r io.ReadSeeker, chunkOffset int64) (*chunk, error) {
	if _, e := r.Seek(chunkOffset, io.SeekStart); e != nil {
		return nil, fmt.Errorf("Failed to seek chunk: %v", e)
	}

	hdr, e := parseHeader(r)
	if e != nil {
		return nil, fmt.Errorf("Failed to parse chunk header: %v", e)
	}

	// To help designing a complex I/O pipeline using Go's io
	// package, let us introduce the following notations:
	//
	// ▶ : a reader
	// > : a writer
	// (file)▶ : an opened file is a reader
	// >(crc32) : a CRC32 hash is a writer
	// >(buf): a bytes.Buffer is a writer
	// (file)▶-copy->(buf) : io.Copy copies content from a file to a buffer
	// ▶(decomp)▶ : a decompressor wraps a reader into another reader
	// >(pipe)▶ : io.Pipe returns a pair of reader and writer
	// ▶(tee)>
	//   ▶      : io.TeeReader takes a reader and a writer and returns a branch
	//
	// The decompressing and checksum pipeline looks as follows:
	//
	// (file)▶-copy->(pipe)▶(tee)>(crc32)
	//                        ▶(decomp)▶-copy->(buf)
	//
	var buf bytes.Buffer
	pr, pw := io.Pipe()
	chksum := crc32.NewIEEE()
	br := io.TeeReader(pr, chksum)
	dflt, e := newDecompressor(br, int(hdr.compressor))
	if e != nil {
		return nil, e
	}

	// Run the pipeline.
	var e1 error
	go func() {
		_, e1 = io.CopyN(pw, r, int64(hdr.compressedSize))
		pw.Close() // End the streaming.
	}()
	_, e = io.Copy(&buf, dflt)

	if e1 != nil {
		return nil, e1
	}
	if e != nil {
		return nil, e
	}
	if hdr.checkSum != chksum.Sum32() {
		return nil, fmt.Errorf("Checksum checking failed.")
	}

	// Parse the chunk from buf.
	ch := &chunk{}
	for i := 0; i < int(hdr.numRecords); i++ {
		var rs [4]byte
		if _, e = buf.Read(rs[:]); e != nil {
			return nil, fmt.Errorf("Failed to read record length: %v", e)
		}

		r := make([]byte, binary.LittleEndian.Uint32(rs[:]))
		if _, e = buf.Read(r); e != nil {
			return nil, fmt.Errorf("Failed to read a record: %v", e)
		}

		ch.records = append(ch.records, r)
		ch.numBytes += len(r)
	}

	return ch, nil
}

func newDecompressor(src io.Reader, compressorID int) (io.Reader, error) {
	switch compressorID {
	case NoCompression:
		return src, nil
	case Snappy:
		return snappy.NewReader(src), nil
	case Gzip:
		return gzip.NewReader(src)
	}
	return nil, fmt.Errorf("Unknown compression algorithm: %d", compressorID)
}
