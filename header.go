package recordio

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	magicNumber uint32 = 0x01020304

	// NoCompression means writing raw chunk data into files.
	// With other choices, chunks are compressed before written.
	NoCompression = 0
	// Snappy had been the default compressing algorithm widely
	// used in Google.  It compromises between speech and
	// compression ratio.
	Snappy = 1
	// Gzip is a well-known compression algorithm.  It is
	// recommmended only you are looking for compression ratio.
	Gzip = 2
)

// Header is the metadata of Chunk.
type Header struct {
	checkSum       uint32
	compressor     uint32
	compressedSize uint32
	len            uint32
}

func (c *Header) write(w io.Writer) (int, error) {
	var buf [20]byte
	binary.LittleEndian.PutUint32(buf[0:4], magicNumber)
	binary.LittleEndian.PutUint32(buf[4:8], c.checkSum)
	binary.LittleEndian.PutUint32(buf[8:12], c.compressor)
	binary.LittleEndian.PutUint32(buf[12:16], c.compressedSize)
	binary.LittleEndian.PutUint32(buf[16:20], c.len)
	return w.Write(buf[:])
}

func parseHeader(r io.Reader) (*Header, error) {
	var buf [20]byte
	if _, e := r.Read(buf[:]); e != nil {
		return nil, e
	}

	if v := binary.LittleEndian.Uint32(buf[0:4]); v != magicNumber {
		return nil, fmt.Errorf("Failed to parse magic number")
	}

	return &Header{
		checkSum:       binary.LittleEndian.Uint32(buf[4:8]),
		compressor:     binary.LittleEndian.Uint32(buf[8:12]),
		compressedSize: binary.LittleEndian.Uint32(buf[12:16]),
		len:            binary.LittleEndian.Uint32(buf[16:20]),
	}, nil
}
