# BenchMark

## Prepare Table Schema

Look at json files in 'benchmark/testdata' and put your table schema into json files. The benchmark script also provides functionality to generate data. The following data types can be generated:

``int``:

- random. default -100~100
- random_range. "range": [min, max]
- increment. same as the auto-increment in MySQL
- random_pool. choose rand value in "pool"

``float``:

- random. default -100.0~100.0
- random_range. "range": [min, max]
- random_pool. choose rand value in "pool"

``string``:

- random_pool. choose rand value in "pool"
- random. same as random_pool but choose rand value in a default pool
- increment. same as the auto-increment in MySQL

## Run Benchmark

put your query into 'benchmark/testdata/query.json' and run the script:

```shell
cd benchmark
make
```

then you can see logs and op cost in 'benchmark/log', or you may `make plot` to create png file.
