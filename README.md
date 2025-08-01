# SCQL

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/secretflow/scql/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/secretflow/scql/tree/main)

Secure Collaborative Query Language (SCQL) is a system that translates SQL statements into Secure Multiparty Computation (SMC) primitives and executes them on a federation of database systems.

![SCQL Workflow](./docs/imgs/scql_workflow.png)

## Documentation

- [Documentation in English](https://www.secretflow.org.cn/en/docs/scql)
- [中文文档](https://www.secretflow.org.cn/zh-CN/docs/scql/)

## Docker Image Release

- Official release docker image: [secretflow/scql](https://hub.docker.com/r/secretflow/scql/tags)
- We also have images at Alibaba Cloud: secretflow-registry.cn-hangzhou.cr.aliyuncs.com/secretflow/scql:[tag]

## Contribution Guidelines

If you would like to contribute to SCQL, please see the [Contribution guidelines](CONTRIBUTING.md).

This documentation also contains instructions for [build and testing](CONTRIBUTING.md#build)

## Hardware Requirements

The following requirements only apply to SCQLEngine.

- CPU
  - x86_64: minimum required AVX instruction set. For FourQ based PSI, the AVX2 instruction set is required.

## Disclaimer

Non-release versions of SCQL are prohibited to use in any production environment due to possible bugs, glitches, lack of functionality, security issues or other problems.

## Citing SCQL

If you think SCQL is helpful for your research or development, please consider citing our [paper](https://www.vldb.org/pvldb/vol17/p3987-fang.pdf):

```text
@article{scql,
  author       = {Wenjing Fang and
                  Shunde Cao and
                  Guojin Hua and
                  Junming Ma and
                  Yongqiang Yu and
                  Qunshan Huang and
                  Jun Feng and
                  Jin Tan and
                  Xiaopeng Zan and
                  Pu Duan and
                  Yang Yang and
                  Li Wang and
                  Ke Zhang and
                  Lei Wang},
  title        = {SecretFlow-SCQL: {A} Secure Collaborative Query pLatform},
  journal      = {Proc. VLDB Endow.},
  volume       = {17},
  number       = {12},
  pages        = {3987--4000},
  year         = {2024},
  url          = {https://www.vldb.org/pvldb/vol17/p3987-fang.pdf},
}
```

## Acknowledgments

- Thanks [TiDB](https://github.com/pingcap/tidb) for providing a powerful SQL parser and planner.
