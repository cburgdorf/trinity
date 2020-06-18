FROM python:3.7

WORKDIR /usr/src/app

COPY . /usr/src/app

# Install deps
RUN apt-get update
RUN apt-get -y install libsnappy-dev gcc g++ cmake

ARG GITREF=9c696e5c545fec670de49866e046a4fd29f3b338

RUN git clone https://github.com/cburgdorf/trinity.git
RUN cd trinity && git checkout $GITREF && pip install -e .[dev] --no-cache-dir

EXPOSE 30303 30303/udp
# Trinity shutdowns aren't yet solid enough to avoid the fix-unclean-shutdown
ENTRYPOINT trinity $EXTRA_OPTS fix-unclean-shutdown && trinity $EXTRA_OPTS
