# RecordIO

[![Build Status](https://travis-ci.org/wangkuiyi/recordio.svg?branch=develop)](https://travis-ci.org/wangkuiyi/recordio) [![GoDoc](https://godoc.org/github.com/wangkuiyi/recordio?status.svg)](https://godoc.org/github.com/wangkuiyi/recordio) [![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

RecordIO is a file format created for [PaddlePaddle Elastic Deep Learning](https://kubernetes.io/blog/2017/12/paddle-paddle-fluid-elastic-learning/).  It is generally useful for distributed computing.

## Motivations

In distributed computing, we often need to dispatch tasks to worker processes.  Usually, a task is defined as a parition of the input data, like what MapReduce and distributed machine learning do.

Most distributed filesystems, including HDFS, Google FS, and CephFS, prefer a small number of big files.  Therefore, it is impratical to create each task as a small file; instead, we need a format for big files that is

1. appenable, so that applications can append records to the file without updating the meta-data, thus fault tolerable,
1. partitionable, so that applications can quickly scan over the file to count the total number of records, and create tasks each corresponds to a sequence of records.

RecordIO is such a file format.

## Write

```go
f, _ := os.Create("a_file.recordio")
w := recordio.NewWriter(f, -1, -1)
w.Write([]byte("Hello"))
w.Write([]byte("World,"))
w.Write([]byte("RecordIO!"))
w.Close()
f.Close()
```

## Read

1. Load chunk index:

   ```go
   f, _ := os.Open("a_file.recordio")
   idx, _ := recordio.LoadIndex(f)
   fmt.Println("Total records: ", idx.NumRecords())
   ```

2. Create one or more scanner to read a range of records.  The
   following example reads 2 records starting from record 1.

   ```go
   s := recordio.NewScanner(f, idx, 1, 2)
   for s.Scan() {
      fmt.Println(string(s.Record()))
   }
   if s.Error() != nil && s.Error() != io.EOF {
      fmt.Println("Something wrong with scanning: %v", s.Error())
   }
   f.Close()
   ```

## Python Wrapper

Python wrapper exposes three classes `Index`, `Writer` and `Scanner`. The interfaces are defined as following:

```python
class Index(object):
   def __init__(self, path):
      """Loads index from file"""
      ...
   
   def num_records(self):
      """Returns total number of records in the file."""
      ...

class Scanner(objec):
   def __init__(self, path, start=0, len=-1, index=None):
      """Creates a scanner for the file. Use the index if provided."""
      ...
   
   def record(self):
      """Returns the current record. Returns None if the end is reached"""
      ...

   def close(self):
      """Closes the scanner"""
      ...

class Writer(object):
   def __init__(self, path):
      """Creates a writer"""
      ...

   def write(self, record):
      """Writes the record to file"""
      ...

   def close(self):
      """Closes the writer"""
      ...
```
