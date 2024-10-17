set -eu

CONTAINER_NAME=$1
LATENCY=$2
BANDWIDTH=$3

docker exec -it ${CONTAINER_NAME} bash -c "set +e; apt-get install iproute2 -y; \
 tc qdisc del dev eth0 root; set -e; \
 tc qdisc add dev eth0 root handle 1: tbf rate ${BANDWIDTH} burst 128kb latency ${LATENCY}; \
 tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 10msec limit 8000"
