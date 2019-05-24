package main

/*
typedef int handle;
*/
import "C"

import "sync"

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
