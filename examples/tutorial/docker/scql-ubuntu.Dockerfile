FROM ubuntu:jammy

ARG TARGETPLATFORM
RUN echo "I'm building for $TARGETPLATFORM"

ENV TZ=Asia/Shanghai

RUN apt update \
    && apt install -y --no-install-recommends libgomp1 curl iputils-ping \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*


COPY ./$TARGETPLATFORM/scqlengine /home/admin/bin/scqlengine
