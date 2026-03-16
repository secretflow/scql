FROM ubuntu:jammy

ARG TARGETPLATFORM
RUN echo "I'm building for $TARGETPLATFORM"

ENV TZ=Asia/Shanghai

RUN apt update \
    && apt upgrade -y \
    && apt install -y libgomp1 \
    && apt install -y curl iputils-ping \
    && apt clean

COPY ./$TARGETPLATFORM/scqlengine /home/admin/bin/scqlengine
