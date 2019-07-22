package recordio

import (
	"os"
	"sort"

	"github.com/wangkuiyi/parallel"
)

type FileList struct {
	files         []string // filename list
	indices       []*Index // index per file
	accumFileLens []int    // accumulative file sizes in records
}

type FileListScanner struct {
	fs         *FileList
	start, end int         // A logical view of the range.
	ch         chan string // From background reading goroutine to Next().
	stop       chan int    // From Close() to the background goroutine.
	err        error
}

// NewFileList build indices of a set of files.
//
// NOTE: If a caller is going to create to FileList objects that scan
// the same set of files, it's the caller's responsibility to make
// sure that the two fn parameters have files in the same order.
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

func NewFileListScanner(fs *FileList, start, len int) *FileListScanner {
	if start < 0 {
		start = 0
	}
	if len < 0 {
		len = fs.TotalRecords()
	}

	rs := &FileListScanner{
		fs:    fs,
		start: start,
		end:   start + len,
		ch:    make(chan string),
		stop:  make(chan int)}

	go func() { rs.err = rs.read() }()
	return rs
}

func (scnr *FileListScanner) read() error {
	// defer close(scnr.ch) // No more emits to ch after read returns,

	// cur := scnr.start
	// file, chunk, record := scnr.fs.Locate(cur)

	// f, e := os.Open(scnr.fs.files[file])
	// if e != nil {
	// 	return e
	// }

	// idx := scnr.fs.indices[file]
	// if _, e := f.Seek(idx.chunkOffsets[chunk], io.SeekStart); e != nil {
	// 	return fmt.Errorf("Failed to seek to chunk: %v", e)
	// }
	return nil
}

func (*FileListScanner) Next() (string, error) {
	return "", nil
}

func (fs *FileListScanner) Err() error {
	return fs.err
}
