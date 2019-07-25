ARG BASE_IMAGE=tensorflow/tensorflow:2.0.0b1-py3
# Use tensorflow/tensorflow:2.0.0b1-gpu-py3 if you want GPU-support.
FROM ${BASE_IMAGE}

RUN apt-get update
RUN apt-get install -y curl git
RUN curl --silent https://dl.google.com/go/go1.12.5.linux-amd64.tar.gz go.tar.gz | tar -C /usr/local -xzf -
ENV PATH=/usr/local/go/bin:$PATH

RUN pip install --upgrade pip
RUN pip install setuptools-golang pytest-runner pytest

VOLUME /go
ENV GOPATH=/go
ENV REPO=$GOPATH/src/github.com/wangkuiyi/recordio

RUN echo "(cd $REPO/python && rm -rf dist && python setup.py bdist_wheel && pip uninstall -y pyrecordio && pip install dist/*.whl)" > /install_recordio.sh
RUN chmod +x /install_recordio.sh

RUN echo "(cd $REPO/examples && /install_recordio.sh && ./synthesize_and_read.py && go run read_synthetic.go)" > /test_recordio.sh
RUN chmod +x /test_recordio.sh

CMD ["bash", "/test_recordio.sh"]
