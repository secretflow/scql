import argparse
import time
import subprocess

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--row",
        type=str,
        required=True
    )
    parser.add_argument(
        "--type",
        type=str,
        required=False,
        default="0",
        help="aggregation algorithm type, 0: default oblivious agg; 1: plaintext agg(reveal groupMark)"
    )

    args = parser.parse_args()

    query = f"""select a.credit_rank, MAX(a.income) as max_income, sum(b.order_amount) as sum_amount FROM (select * from ta limit {args.row}) as a INNER JOIN (select * from tb limit {args.row}) as b on a.ID = b.ID group by a.credit_rank"""
    cwd = f"""docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl create job --query  "{query}" --agg-type {args.type}  --project-id "demo" --host http://localhost:8080 --timeout 10' """
    p = subprocess.run(cwd, stdout=subprocess.PIPE, shell=True)
    assert p.returncode == 0, p.stdout.decode("utf-8")
    handler = ""
    for line in p.stdout.splitlines():
        tmp = line.decode("utf-8")
        if tmp.find("get result") != -1:
            handler = tmp.rstrip('\r\n')

    assert handler != "", p.stdout.decode("utf-8")
    print(handler)

    while True:
        pt = subprocess.run(f"docker exec -it vldb-broker_alice-1 bash -c '{handler}'", shell=True, stdout=subprocess.PIPE)
        assert pt.returncode == 0, pt.stdout.decode("utf-8")
        def not_ready(line):
            if line.find("not ready") != -1:
                print("not ready")
                return True
            else:
                print(line)
                return False
        if not_ready(pt.stdout.decode("utf-8")):
            time.sleep(5)
        else:
            break

    grep_str = "finished executing node(sum\|finished executing node(max\|finished executing node(shuffle"
    pf = subprocess.run(f"""docker exec -it vldb-engine_alice-1 grep "{grep_str}" logs/sciengine.log | tail -3""", shell=True, stdout=subprocess.PIPE)
    print(pf.stdout.decode("utf-8"))

if __name__ == "__main__":
    main()