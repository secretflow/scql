FROM registry.openanolis.cn/openanolis/anolisos:8.6

ENV TZ=Asia/Shanghai

# update os infra
RUN dnf update -y && dnf upgrade -y && dnf clean all

RUN yum install -y iproute-tc
# libgomp is used in spu to speed up psi.
RUN yum install -y libgomp

COPY ./scqlengine /home/admin/bin/scqlengine
COPY ./scdbserver /home/admin/bin/scdbserver
COPY ./scdbclient /home/admin/bin/scdbclient