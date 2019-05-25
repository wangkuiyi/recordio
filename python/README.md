# Build and Test `recordio` Package

## Build

We rely on [setuptools-golang](https://github.com/asottile/setuptools-golang) extension to build the binary wheel package. In this directory, just run:

```
pip wheel .
```

## Local Test

First install the wheel package locally:

```
pip install -I pyrecordio-<version>-<os>.whl
```

Then run the tests:

```
python recordio/tests/recordio_test.py
```
