tc qdisc del dev lo root
tc qdisc add dev lo root handle 1: tbf rate 100mbit burst 128kb latency 10ms
tc qdisc add dev lo parent 1:1 handle 10: netem delay 10msec limit 8000