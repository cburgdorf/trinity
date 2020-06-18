FROM python:3.7

WORKDIR /usr/src/app

COPY . /usr/src/app

# Install deps
RUN apt-get update
RUN apt-get -y install libsnappy-dev gcc g++ cmake

ARG GITREF=674c645d77aaf5500d69016ae6b148e3046a1799

RUN git clone https://github.com/cburgdorf/trinity.git
RUN cd trinity && git checkout $GITREF && pip install -e .[dev] --no-cache-dir

EXPOSE 30303 30303/udp
# Trinity shutdowns aren't yet solid enough to avoid the fix-unclean-shutdown
ENTRYPOINT trinity $EXTRA_OPTS fix-unclean-shutdown && trinity $EXTRA_OPTS
