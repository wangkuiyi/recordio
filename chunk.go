package recordio

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"

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

// write a chunk, including the header and compressed chunk data.
func (ch *chunk) write(w io.Writer, compressorID int) error {
	// NOTE: don't check ch.numBytes as we allow empty records.
	if len(ch.records) == 0 {
		return nil
	}

	var buf bytes.Buffer
	chksum, e := ch.compress(compressorID, &buf)
	if e != nil {
		return e
	}

	// Write chunk header and compressed data.
	hdr := &header{
		checkSum:       chksum,
		compressor:     uint32(compressorID),
		compressedSize: uint32(buf.Len()),
		numRecords:     uint32(len(ch.records)),
	}
	if _, e := hdr.write(w); e != nil {
		return fmt.Errorf("Failed to write chunk header: %v", e)
	}
	if _, e := w.Write(buf.Bytes()); e != nil {
		return fmt.Errorf("Failed to write chunk data: %v", e)
	}

	// Clear the current chunk.
	ch.records = nil
	ch.numBytes = 0

	return nil
}

// compress chunk data (records) into a buffer and returns the CRC32 checksum.
func (ch *chunk) compress(compressorID int, buf *bytes.Buffer) (uint32, error) {
	// In addition to notations introduced in the function
	// definition of read, we add the following:
	//
	// >(buf) : a bytes.Buffer is a writer.
	// >(compr)> : a compressor wraps a writer into another writer.
	//
	// Then, the pipeline of dumping a chunk looks like the
	// following:
	//
	// write->(compr)>(crc32, buf)
	chksum := crc32.NewIEEE()
	mw := io.MultiWriter(buf, chksum)
	compr := newCompressor(mw, compressorID)

	// Write raw records and their lengths into data buffer.
	for _, r := range ch.records {
		var rs [4]byte
		binary.LittleEndian.PutUint32(rs[:], uint32(len(r)))

		if _, e := compr.Write(rs[:]); e != nil {
			return 0, fmt.Errorf("Failed to write record length: %v", e)
		}

		if _, e := compr.Write(r); e != nil {
			return 0, fmt.Errorf("Failed to write record: %v", e)
		}
	}
	if e := compr.Close(); e != nil {
		return 0, fmt.Errorf("Failed to close compressor: %v", e)
	}

	return chksum.Sum32(), nil
}

// TODO: use ioutil.WriteNopCloser once the following PR is in public release:
// https://go-review.googlesource.com/c/go/+/175779#message-31dfdd1aaee623f9e80fb652af7bd0cc8cc4fcc3
type writeNopCloser struct {
	io.Writer
}

func (writeNopCloser) Close() error { return nil }

func newCompressor(w io.Writer, compressorID int) io.WriteCloser {
	switch compressorID {
	case NoCompression:
		return writeNopCloser{w}
	case Snappy:
		return snappy.NewWriter(w)
	case Gzip:
		return gzip.NewWriter(w)
	default:
		log.Fatalf("Unknown compressor ID: %d", compressorID)
	}
	return nil
}

// readChunk from r into the memory.
func readChunk(r io.Reader) (*chunk, error) {
	hdr, e := parseHeader(r)
	if e != nil {
		return nil, e // NOTE: must return e literally as required by FileListScanner.
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
	//                        ▶(decomp)▶-read
	//
	pr, pw := io.Pipe()
	chksum := crc32.NewIEEE()
	br := io.TeeReader(pr, chksum)
	decomp, e := newDecompressor(br, int(hdr.compressor))
	if e != nil {
		return nil, e
	}

	// Intake data.
	var e1 error
	go func() {
		defer pw.Close()
		_, e1 = io.CopyN(pw, r, int64(hdr.compressedSize))
	}()

	// Outtake data.
	ch := &chunk{}
	for i := 0; i < int(hdr.numRecords); i++ {
		var rs [4]byte
		if _, e = io.ReadFull(decomp, rs[:]); e != nil {
			return nil, fmt.Errorf("Failed to read record length: %v", e)
		}

		l := int(binary.LittleEndian.Uint32(rs[:]))
		r := make([]byte, l)
		_, e := io.ReadFull(decomp, r)
		if e != nil {
			return nil, fmt.Errorf("Failed to read a record: %v", e)
		}
		ch.records = append(ch.records, r)
		ch.numBytes += len(r)
	}

	if e1 != nil {
		return nil, e1
	}

	if hdr.checkSum != chksum.Sum32() {
		return nil, fmt.Errorf("Checksum checking failed. %d vs %d", hdr.checkSum, chksum.Sum32())
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
