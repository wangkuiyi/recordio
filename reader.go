package recordio

import (
	"io"
	"log"
	"sort"
)

// Index consists offsets and sizes of the consequetive chunks in a RecordIO file.
type Index struct {
	chunkOffsets   []int64
	accumChunkLens []int // accumulative chunk sizes for binary search.
	numRecords     int   // the number of all records in a file.
	chunkRecords   []int // the number of records in chunks.
}

// LoadIndex scans the file and parse chunkOffsets, chunkLens, and len.
func LoadIndex(r io.ReadSeeker) (*Index, error) {
	f := &Index{}
	offset := int64(0)
	accum := 0
	var e error
	var hdr *header

	for {
		hdr, e = parseHeader(r)
		if e != nil {
			break
		}

		f.chunkOffsets = append(f.chunkOffsets, offset)
		accum += int(hdr.numRecords)
		f.accumChunkLens = append(f.accumChunkLens, accum)
		f.chunkRecords = append(f.chunkRecords, int(hdr.numRecords))
		f.numRecords += int(hdr.numRecords)

		offset, e = r.Seek(int64(hdr.compressedSize), io.SeekCurrent)
		if e != nil {
			break
		}
	}

	if e == io.EOF {
		return f, nil
	}
	return nil, e
}

// NumRecords returns the total number of records in a RecordIO file.
func (r *Index) NumRecords() int {
	return r.numRecords
}

// NumChunks returns the total number of chunks in a RecordIO file.
func (r *Index) NumChunks() int {
	return len(r.accumChunkLens)
}

// Locate returns the index of chunk that contains the given record,
// and the record index within the chunk.  It returns (-1, -1) if the
// record is out of range.
func (r *Index) Locate(recordIndex int) (int, int) {
	chunk := sort.Search(len(r.accumChunkLens), func(i int) bool {
		return recordIndex < r.accumChunkLens[i]
	})
	if chunk >= r.NumChunks() {
		return -1, -1
	}

	prevAccum := 0
	if chunk > 0 {
		prevAccum = r.accumChunkLens[chunk-1]
	}
	return chunk, recordIndex - prevAccum
}

// Scanner scans records in a specified range within [0, numRecords).
type Scanner struct {
	reader          io.ReadSeeker
	index           *Index
	start, end, cur int
	chunkIndex      int
	chunk           *chunk
	err             error
}

// NewScanner creates a scanner that sequencially reads records in the
// range [start, start+len).  If start < 0, it scans from the
// beginning.  If len < 0, it scans till the end of file.
func NewScanner(r io.ReadSeeker, index *Index, start, len int) *Scanner {
	if start < 0 {
		start = 0
	}
	if len < 0 || start+len >= index.NumRecords() {
		len = index.NumRecords() - start
	}

	return &Scanner{
		reader:     r,
		index:      index,
		start:      start,
		end:        start + len,
		cur:        start - 1, // The intial status required by Scan.
		chunkIndex: -1,
		chunk:      &chunk{},
	}
}

// Scan moves the cursor forward for one record and loads the chunk
// containing the record if not yet.
func (s *Scanner) Scan() bool {
	s.cur++

	if s.cur >= s.end {
		s.err = io.EOF
	} else {
		if ci, _ := s.index.Locate(s.cur); s.chunkIndex != ci {
			s.chunkIndex = ci
			if _, e := s.reader.Seek(s.index.chunkOffsets[ci], io.SeekStart); e != nil {
				log.Printf("Failed to seek chunk: %v", e)
				return false
			}
			s.chunk, s.err = readChunk(s.reader)
		}
	}

	return s.err == nil
}

// Record returns the record under the current cursor.
func (s *Scanner) Record() []byte {
	_, ri := s.index.Locate(s.cur)
	return s.chunk.records[ri]
}

// Error returns the error that stopped Scan.
func (s *Scanner) Error() error {
	return s.err
}
