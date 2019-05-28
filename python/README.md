# Build and Test `recordio` Package

## Build

Make sure you have python version 3.5 or newer and golang compiler install on your machine. Or you could use the [Dockerfile](../docker/Dockerfile) provided to build a Docker image.

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
