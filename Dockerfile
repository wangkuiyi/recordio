FROM python:3.7

RUN apt-get update
RUN apt-get install -y curl
RUN curl --silent https://dl.google.com/go/go1.12.5.linux-amd64.tar.gz go.tar.gz | tar -C /usr/local -xzf -
ENV PATH=/usr/local/go/bin:$PATH

RUN pip install --upgrade pip

RUN echo "#!/bin/bash" > /build.bash
RUN echo "pip wheel /work/python" >> /build.bash
RUN chmod +x /build.bash
CMD ["/build.bash"]
