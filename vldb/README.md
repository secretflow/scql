### TLDR

This folder provides an experimental environment to facilitate the reproduction of experiments related to the VLDB paper

note:

- To simplify deployment, the environment is a single-machine deployment.
- It is recommended to run on a physical machine with 32 vCPU @2.5GHz(Intel Xeon 8269CY) and 256GB RAM.

### Prerequisites

#### Build image for test

The images on Dockerhub are mainly used for production and lack some testing features. Therefore, you need to compile the test images yourself to complete all test.

```bash
# run in the repo directory
bash docker/build.sh -c -t vldb
```

#### Prepare test data

Generate data for SCQL Operator test

```bash
cd vldb/scql-operator-test
mkdir ../docker-compose/data/
python generate_data.py
python tpch_mock.py -r 10000000
```

#### Turn up containers and set up project

```bash
cd ../docker-compose
bash setup.sh
# NOTE: wait until all containers are running, which may take several minutes to load data.
docker compose -p vldb up -d

# set up project
bash project_bootstrap.sh
```

- If you encounter port conflict problem, you could change the default env `XXX_PORT` in `vldb/docker-compose/.env` file.
- Please wait until all containers are running successfully, since loading data may cost several miniutes, you can check the status by `docker compose -p vldb ps`

### Run experiments

If you want to run in WAN, please use the following command

```bash
# go back to the repo directory
cd ../..
bash vldb/setup_wan.sh
# restore to LAN, run the following
# bash vldb/setup_wan.sh -r
```

#### SCQL Operator Test

Under directory 'vldb/scql-operator-test', you can try the experiment for SCQL JOIN operator using the following command:

```bash
# test join operator, for more details, using flag --help
python3 vldb/scql-operator-test/join.py --row 1000000 --type 0
```

You can check the cost time from the output, or using **docker logs -f vldb-engine_alice-1** to get more detail

```bash
2024-06-13 02:15:48.581 [info] [engine_service_impl.cc:RunPlanCore:460] [sciengine] session(d2b31f80-292a-11ef-aba8-0242ac1a0005) finished executing node(join.2), op(Join), cost(10868)ms
```

#### TPCH Benchmark

First step, turn up containers

```bash
# go back to the repo directory
# clean up containers
docker compose -p vldb down
# restart containers
bash vldb/docker-compose/setup.sh
# NOTE: wait until all containers are running, which may take several minutes to load data.
(cd vldb/docker-compose && docker compose -p vldb up -d)
bash vldb/setup_wan.sh
# restore to LAN, run the following
# bash vldb/setup_wan.sh -r
```

Second step, prepare tables using by benchmark

```bash
bash vldb/tpch_scripts/prepare.sh
```

Third step, run benchmark

```bash
# run.sh may take several hours to finish, you may need to run it in background
bash vldb/tpch_scripts/run.sh
```

Finally, clean containers

```bash
docker compose -p vldb down
```

#### SP/Sort Test

SP/Sort is implemented in SPU. To test SP/Sort operators, here we provide a simulation demo to benchmark SP/Sort performance under WAN. To run this demo, please follow the [instructions](https://github.com/secretflow/spu/blob/main/CONTRIBUTING.md#linux) to prepare SPU build environment.

First step, setup network condition.

```bash
# go back to the repo directory
bash vldb/sort-test/setup_wan.sh
```

Second step, run the demo, --numel is the number of elements to sort/permute.

```bash
bazel run //vldb/sort-test:sort -c opt -- --numel=1000
```
