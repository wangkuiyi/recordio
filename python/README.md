# Python Binding of `recordio`

This package provides a Python binding of the recordio package in Go.

## API

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

## Build

To build this Python binding, we need Python 3 and the Go compiler.  If you don't want to install them locally, you can install them into a Docker image, and run the image to build the package.

To build the Docker image, we can use the `/Dockerfile`.

```bash
docker build -t recordio:dev .
```

To build the package using the Docker image, run the following command:

```bash
docker run --rm -it -v $PWD:/work recordio:dev
```

## Test

First install the wheel package locally:

```
pip install -I pyrecordio-<version>-<os>.whl
```

Then run the tests:

```
python recordio/tests/recordio_test.py
```
