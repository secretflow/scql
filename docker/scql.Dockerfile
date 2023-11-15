FROM openanolis/anolisos:8.6 as base

ENV TZ=Asia/Shanghai

# update os infra
RUN dnf update -y && dnf upgrade -y && dnf clean all

RUN yum install -y iproute-tc
# libgomp is used in spu to speed up psi.
RUN yum install -y libgomp

COPY ./scqlengine /home/admin/bin/scqlengine
COPY ./scdbserver /home/admin/bin/scdbserver
COPY ./scdbclient /home/admin/bin/scdbclient
COPY ./broker /home/admin/bin/broker
COPY ./brokerctl /home/admin/bin/brokerctl

FROM base as image-dev

RUN yum install -y wget
RUN wget https://go.dev/dl/go1.21.4.linux-amd64.tar.gz \
  && rm -rf /usr/local/go \
  && tar -C /usr/local -xzf go1.21.4.linux-amd64.tar.gz \
  && rm -f go1.21.4.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}" 

#install dlv
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Add GOPATH to PATH
ENV PATH="${PATH}:/root/go/bin"

FROM base as image-prod