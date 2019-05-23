# RecordIO

RecordIO is a file format created for [PaddlePaddle Elastic Deep Learning](https://kubernetes.io/blog/2017/12/paddle-paddle-fluid-elastic-learning/).  It is generally useful for distributed computing.

## Motivations

In distributed computing, we often need to dispatch tasks to worker processes.  Usually, a task is defined as a parition of the input data, like what MapReduce and distributed machine learning do.

Most distributed filesystems, including HDFS, Google FS, and CephFS, prefer a small number of big files.  Therefore, it is impratical to create each task as a small file; instead, we need a format for big files that is

1. appenable, so that applications can append records to the file without updating the meta-data, thus fault tolerable,
1. partitionable, so that applications can quickly scan over the file to count the total number of records, and create tasks each corresponds to a sequence of records.

RecordIO is such a file format.

## Write

```go
f, e := os.Create("a_file.recordio")
w := recordio.NewWriter(f)
w.Write([]byte("Hello"))
w.Write([]byte("World!"))
w.Close()
```

## Read

1. Load chunk index:

   ```go
   f, e := os.Open("a_file.recordio")
   idx, e := recordio.LoadIndex(f)
   fmt.Println("Total records: ", idx.Len())
   ```

2. Create one or more scanner to read a range of records.  The
   following example reads the range
   [1, 3), i.e., the second and the third records:

   ```go
   f, e := os.Open("a_file.recordio")
   s := recrodio.NewScanner(f, idx, 1, 3)
   for s.Scan() {
      fmt.Println(string(s.Record()))
   }
   if s.Err() != nil && s.Err() != io.EOF {
      log.Fatalf("Something wrong with scanning: %v", e)
   }
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
