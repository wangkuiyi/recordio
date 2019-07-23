package recordio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/wangkuiyi/parallel"
)

type FileList struct {
	files         []string // filename list
	indices       []*Index // index per file
	accumFileLens []int    // accumulative file sizes in records
}

// NewFileList builds indices of a set of files.
//
// NOTE: If a caller is going to create more than one FileList objects
// that scan the same set of files, the caller must make sure that
// they have the same file list of the same order in parameter fn.
func NewFileList(fn []string) (*FileList, error) {
	idcs := make([]*Index, len(fn))

	if e := parallel.For(0, len(fn), 1, func(i int) error {
		f, e := os.Open(fn[i])
		if e != nil {
			return e
		}
		defer f.Close()

		idcs[i], e = LoadIndex(f)
		return e
	}); e != nil {
		return nil, e
	}

	accum := 0
	accumFileLens := make([]int, len(fn))
	for i, idx := range idcs {
		accum += idx.NumRecords()
		accumFileLens[i] = accum
	}

	return &FileList{
		files:         fn,
		indices:       idcs,
		accumFileLens: accumFileLens}, nil
}

func (fs *FileList) Locate(recordIndex int) (file, chunk, record int) {
	file = sort.Search(len(fs.accumFileLens), func(i int) bool {
		return recordIndex < fs.accumFileLens[i]
	})
	if file >= len(fs.files) {
		return -1, -1, -1
	}

	prevAccum := 0
	if chunk > 0 {
		prevAccum = fs.accumFileLens[file-1]
	}

	chunk, record = fs.indices[file].Locate(recordIndex - prevAccum)
	return file, chunk, record

}

func (fs *FileList) TotalRecords() int {
	if len(fs.accumFileLens) > 0 {
		return fs.accumFileLens[len(fs.accumFileLens)-1]
	}
	return 0
}

var (
	ErrStopped = errors.New("FileListScanner.Close() stopped scanning")
)

type FileListScanner struct {
	fl         *FileList
	start, end int         // A logical view of the range.
	ch         chan []byte // From background reading goroutine to Next().
	stop       chan int    // From Close() to the background goroutine.
	err        error
}

func NewFileListScanner(fl *FileList, start, len int) *FileListScanner {
	if start < 0 {
		start = 0
	}
	if len < 0 || start+len > fl.TotalRecords() {
		len = fl.TotalRecords() - start
	}

	rs := &FileListScanner{
		fl:    fl,
		start: start,
		end:   start + len,
		ch:    make(chan []byte, 1000), // Buffer size is critial to performance. Currently ad-hoc.
		stop:  make(chan int)}

	go func() { rs.err = rs.scan() }()
	return rs
}

func (scnr *FileListScanner) scan() error {
	defer close(scnr.ch) // No more emits to ch after read returns,

	cur := scnr.start
	file, chunk, record := scnr.fl.Locate(cur)
	for cur < scnr.end {
		n, e := scnr.scanFile(file, chunk, record, scnr.end-cur)
		if e != nil && e != io.EOF {
			return e
		}
		cur += n
		file++
		chunk, record = 0, 0 // Since the second file, read from the first record.
	}
	return nil
}

// scanFile reads at most todo records from the record-th in chunk of
// file.  It returns when it reaches the end of the file or having
// read enough number of records.  In either case, it returns the
// number of read records.
func (scnr *FileListScanner) scanFile(file, chunk, record, todo int) (done int, err error) {
	f, e := os.Open(scnr.fl.files[file])
	if e != nil {
		return 0, e
	}
	defer f.Close()

	idx := scnr.fl.indices[file]
	if _, e := f.Seek(idx.chunkOffsets[chunk], io.SeekStart); e != nil {
		return 0, fmt.Errorf("Failed to seek to chunk: %v", e)
	}

	for todo > 0 {
		n, e := scnr.scanChunk(f, record, todo)
		todo -= n
		done += n
		if e != nil {
			return done, e
		}
		record = 0 // Since the second chunk, we read since its first record.
	}

	return done, nil
}

// scanChunk reads r for a chunk and returns at most todo records
// starting from the record-th.
func (scnr *FileListScanner) scanChunk(r io.Reader, record, todo int) (done int, err error) {
	chnk, e := readChunk(r)
	if e != nil {
		return done, e
	}

	for i := record; todo > 0 && i < len(chnk.records); i++ {
		select {
		case <-scnr.stop:
			return done, ErrStopped

		default:
			scnr.ch <- chnk.records[i]
			done++
			todo--
		}
	}
	return done, nil
}

func (fl *FileListScanner) Chan() chan ([]byte) {
	return fl.ch
}

func (fl *FileListScanner) Error() error {
	return fl.err
}
