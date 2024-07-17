set -eu

RESTORE_FLAG=false

usage() {
  echo "Usage: $0 [-r]"
  echo "default set 100Mbps and 20ms RTT for engine"
  echo "Options:"
  echo "  -r, restore to LAN."
}

while getopts "r" options; do
  case "${options}" in
  r)
    RESTORE_FLAG=true
    ;;
  *)
    usage
    exit 1
    ;;
  esac
done

if $RESTORE_FLAG; then
  docker exec -it vldb-engine_alice-1 bash -c 'tc qdisc del dev eth0 root'
  docker exec -it vldb-engine_bob-1 bash -c 'tc qdisc del dev eth0 root'
  echo "restore LAN for engine"
  exit
fi

echo "set 100Mbps and 20ms RTT for engine"
docker exec -it vldb-engine_alice-1 bash -c 'set +e; apt-get install iproute2 -y; \
 tc qdisc del dev eth0 root; set -e; \
 tc qdisc add dev eth0 root handle 1: tbf rate 100mbit burst 128kb latency 10ms; \
 tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 10msec limit 8000'

docker exec -it vldb-engine_bob-1 bash -c 'set +e; apt-get install iproute2 -y; \
 tc qdisc del dev eth0 root; set -e; \
 tc qdisc add dev eth0 root handle 1: tbf rate 100mbit burst 128kb latency 10ms; \
 tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 10msec limit 8000'