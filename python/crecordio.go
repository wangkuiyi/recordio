// Note: this file is part of python extension to recordio. It should be put
// in the same directory of setup.py so pip to find it.
package main

/*
#include <string.h>
#include <stdlib.h>
typedef int handle;
*/
import "C"

import (
	"log"
	"os"
	"sync"
	"unsafe"

	"github.com/wangkuiyi/recordio"
)

var mu sync.Mutex
var handleMap = make(map[C.handle]interface{})
var curHandle C.handle

func addObject(r interface{}) C.handle {
	mu.Lock()
	defer mu.Unlock()
	handle := curHandle
	curHandle++
	handleMap[handle] = r
	return handle
}

func getObject(handle C.handle) interface{} {
	mu.Lock()
	defer mu.Unlock()
	return handleMap[handle]
}

func removeObject(handle C.handle) interface{} {
	mu.Lock()
	defer mu.Unlock()
	r := handleMap[handle]
	delete(handleMap, handle)
	return r
}

var nullPtr = unsafe.Pointer(uintptr(0))

type writer struct {
	w *recordio.Writer
	f *os.File
}

type index struct {
	idx *recordio.Index
}

type scanner struct {
	s *recordio.Scanner
	f *os.File
}

//export create_recordio_writer
func create_recordio_writer(path *C.char) C.handle {
	p := C.GoString(path)
	f, err := os.Create(p)
	if err != nil {
		log.Println(err)
		return -1
	}

	w := recordio.NewWriter(f, -1, -1)
	writer := writer{f: f, w: w}
	return addObject(writer)
}

//export recordio_write
func recordio_write(h C.handle, buf *C.uchar, size C.int) C.int {
	w := getObject(h).(writer)

	// Make a copy of the C buffer rather than create a slice
	// backed by the C buffer. This is because RecordIO caches the
	// slice in memory until the max chunk size is reached and
	// then dump the slice to disk. At which point the C buffer is
	// no longer valid.
	b := make([]byte, int(size))
	for i := 0; i < int(size); i++ {
		ptr := (*C.uchar)(unsafe.Pointer(uintptr(unsafe.Pointer(buf)) + uintptr(i)))
		b[i] = byte(*ptr)
	}

	c, err := w.w.Write(b)
	if err != nil {
		log.Println(err)
		return -1
	}
	return C.int(c)
}

//export create_recordio_index
func create_recordio_index(path *C.char) C.handle {
	p := C.GoString(path)
	f, err := os.Open(p)
	if err != nil {
		log.Println(err)
		return -1
	}

	defer f.Close()

	i, err := recordio.LoadIndex(f)
	if err != nil {
		log.Println(err)
		return -1
	}
	idx := index{idx: i}
	return addObject(idx)
}

//export recordio_index_num_records
func recordio_index_num_records(h C.handle) C.int {
	idx := getObject(h).(index)

	return C.int(idx.idx.NumRecords())
}

//export create_recordio_reader
func create_recordio_reader(path *C.char, h C.handle, start, len int) C.handle {
	idx := getObject(h).(index)

	p := C.GoString(path)
	f, err := os.Open(p)
	if err != nil {
		log.Println(err)
		return -1
	}

	s := recordio.NewScanner(f, idx.idx, start, len)
	r := scanner{s: s, f: f}
	return addObject(r)
}

//export recordio_read
func recordio_read(h C.handle, record **C.uchar) C.int {
	r := getObject(h).(scanner)
	if r.s.Scan() {
		buf := r.s.Record()
		if len(buf) == 0 {
			*record = (*C.uchar)(nullPtr)
			return 0
		}

		*record = (*C.uchar)(unsafe.Pointer(&buf[0]))
		return C.int(len(buf))
	}

	return -1
}

//export release_object
func release_object(h C.handle) {
	obj := removeObject(h)
	switch o := obj.(type) {
	case writer:
		o.w.Close()
		o.f.Close()
	case scanner:
		o.f.Close()
	case index:
	default:
		panic(o)
	}
}

func main() {} // Required but ignored
