ARG BASE_IMAGE=tensorflow/tensorflow:2.0.0b1-py3
# Use tensorflow/tensorflow:2.0.0b1-gpu-py3 if you want GPU-support.
FROM ${BASE_IMAGE}

RUN apt-get update
RUN apt-get install -y curl git
RUN curl --silent https://dl.google.com/go/go1.12.5.linux-amd64.tar.gz go.tar.gz | tar -C /usr/local -xzf -
ENV PATH=/usr/local/go/bin:$PATH

RUN pip install --upgrade pip
RUN pip install setuptools-golang pytest-runner pytest

VOLUME /work

RUN echo "(cd /work/python && python setup.py bdist_wheel && pip uninstall recordio && pip install dist/*.whl)" > /install_recordio.sh
RUN chmod +x /install_recordio.sh

RUN echo "(cd /work/python/examples && /install_recordio.sh && PYTHONPATH=$PYTHONPATH:. ./synthesize_and_read.py)" > /test_recordio.sh
RUN chmod +x /test_recordio.sh

CMD ["bash", "/test_recordio.sh"]
