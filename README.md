# RecordIO

[![Build Status](https://travis-ci.org/wangkuiyi/recordio.svg?branch=develop)](https://travis-ci.org/wangkuiyi/recordio) [![GoDoc](https://godoc.org/github.com/wangkuiyi/recordio?status.svg)](https://godoc.org/github.com/wangkuiyi/recordio) [![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

RecordIO is a file format created for [PaddlePaddle Elastic Deep Learning](https://kubernetes.io/blog/2017/12/paddle-paddle-fluid-elastic-learning/).  It is generally useful for distributed computing.

## Motivations

### Static Sharding v.s. Dynamic Sharding

In distributed computing, we often need to partition and dispatch data to worker processes.  A commonly-used solution, known as *static sharding*, is to define each data *shard* as a file and to map each file to a worker process.  However, when we are doing fault-tolerant distributed computing or elastic scheduling of distributed computing jobs, the total number of worker processes might change at runtime, and static sharding doesn't work.  In such cases, we want to partition records in a file into data shards -- an approach known as *dynamic sharding*.

### ReocrdIO and Dynamic Sharding

We define RecordIO file format to support dynamic sharding.  A RecordIO file consists of a sequence of records grouped by chunks.  We could build an index of records by reading through a file quickly while skipping over chunks.  We then use this index data structure to seek to the beginning of any record.  In this way, we can locate any dynamic shard efficiently.

## The Go API

### Writing

```go
f, _ := os.Create("a_file.recordio")
w := recordio.NewWriter(f, -1, -1)
w.Write([]byte("Hello"))
w.Write([]byte("World,"))
w.Write([]byte("RecordIO!"))
w.Close()
f.Close()
```

## Reading

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

## The Python Binding

We provide a Python binding of the Go implementation.  For more information please refer to [`python/README.md`](python/README.md).
