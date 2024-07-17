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
        help="group by type, 0: default vertical groupBy; 1: oblivious groupBy; 2: plaintext groupBy(plaintext ccl will be set)"
    )

    args = parser.parse_args()

    if args.type == "2":
        print("temporary set plaintext ccl for join key in plaintext agg")
        pp = subprocess.run("""docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8080'""", stdout=subprocess.PIPE, shell=True)
        assert pp.returncode == 0, pp.stdout.decode("utf-8")


    query = f"""select ta.credit_rank, tb.is_active, count(*) as cnt FROM (select * from ta limit {args.row}) as ta INNER JOIN (select * from tb limit {args.row}) as tb on ta.ID = tb.ID group by ta.credit_rank, tb.is_active;"""
    cwd = f"""docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl create job --query  "{query}" --sgb-type {args.type}  --project-id "demo" --host http://localhost:8080 --timeout 10' """
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

    grep_str = "finished executing node(sort\|finished executing node(group_mark\|finished executing node(group"
    pf = subprocess.run(f"""docker exec -it vldb-engine_alice-1 grep "{grep_str}" logs/sciengine.log | tail -2""", shell=True, stdout=subprocess.PIPE)
    print(pf.stdout.decode("utf-8"))

    if args.type == "2":
        pp = subprocess.run("""docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT_AFTER_GROUP_BY --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8080'""", stdout=subprocess.PIPE, shell=True)
        assert pp.returncode == 0, pp.stdout.decode("utf-8")

if __name__ == "__main__":
    main()