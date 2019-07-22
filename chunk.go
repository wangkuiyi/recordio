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
	// write->(compr)>(pipe)▶(tee)>(crc32)
	//                        ▶-copy->(buf)
	pr, pw := io.Pipe()
	compr := newCompressor(pw, compressorID)
	chksum := crc32.NewIEEE()
	br := io.TeeReader(pr, chksum)

	// Write raw records and their lengths into data buffer.
	var e1 error
	go func() {
		defer func() {
			compr.Close() // Flush data out.
			pw.Close()    // Notify the end.
		}()
		for _, r := range ch.records {
			var rs [4]byte
			binary.LittleEndian.PutUint32(rs[:], uint32(len(r)))

			if _, e := compr.Write(rs[:]); e != nil {
				e1 = fmt.Errorf("Failed to write record length: %v", e)
				return
			}

			if _, e := compr.Write(r); e != nil {
				e1 = fmt.Errorf("Failed to write record: %v", e)
				return
			}
		}
	}()

	_, e := io.Copy(buf, br)
	if e != nil {
		return 0, e
	}
	return chksum.Sum32(), e1
}

func newCompressor(w io.WriteCloser, compressorID int) io.WriteCloser {
	switch compressorID {
	case NoCompression:
		return w
	case Snappy:
		return snappy.NewWriter(w)
	case Gzip:
		return gzip.NewWriter(w)
	}
	return nil
}

// read a chunk from r at the given offset.
func read(r io.Reader) (*chunk, error) {
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
		if _, e = decomp.Read(rs[:]); e != nil {
			return nil, fmt.Errorf("Failed to read record length: %v", e)
		}

		l := binary.LittleEndian.Uint32(rs[:])
		r := make([]byte, l)
		if _, e = decomp.Read(r); e != nil {
			if !(e == io.EOF && l == 0 && i == int(hdr.numRecords)-1) {
				// Read returns EOF if an "" is at the end of a chunk.
				return nil, fmt.Errorf("Failed to read a record: %v", e)
			}
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
