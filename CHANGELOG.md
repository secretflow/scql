# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Types of changes

`Added` for new features.
`Changed` for changes in existing functionality.
`Deprecated` for soon-to-be removed features.
`Removed` for now removed features.
`Fixed` for any bug fixes.
`Security` in case of vulnerabilities.

## Staging

### Added

### Changed

### Fixed

## [0.9.1] - 2024-10-16

### Added
- Support window function `ROW_NUMBER()` with PARTITION BY clause and ORDER BY clause.
- Add new CCL constraint `REVAL_RANK`.
- Add ExplainQuery API with path `/intra/query/explain`.
- Support `INSERT INTO SELECT` syntax to allow writing query result back to db (mysql/sqlite/postgres).
- Support `trim` function.

### Changed

- Improved the job watcher to work better in broker clustered mode.


## [0.9.0] - 2024-08-01

### Added
- Support write outfile to OSS/MINIO via `select into` query.
- Support `sin`, `cos`, `acos` function.
- Support `geodist` function.
- Broker support using postgres as metadata storage.

### Changed
- Reduce the memory peak of large-scale intersection tasks through streaming execution.
- Link tcmalloc to solve the problem of memory increase.

### Fixed
- Fix crashes when dumpfile exceeds 2GB string column.
- Reduce the probability of graph checksum inconsistency issues.

## [0.8.1] - 2024-07-02

### Added

- Support session-based log isolation functioality in the SCQL Engine.
- Support consul-based broker registration/discovery services, providing ACL/TLS authentication.

### Changed

### Fixed

## [0.8.0] - 2024-06-12

### Added

- Enhanced `FetchResult` RPC and `brokerctl get result` command to report job progress when result is not ready.
- Support project/query level configs
- Support NULL for private data: including Arithmetic, Logic, Aggregation, etc., {IS [NOT] NULL, IFNULL, COALESCE} are also supported.
- Support port isolation for engine link service and control panel service (RunExecutionPlan).
- Add new CCL constraint `PLAINTEXT_AS_JOIN_PAYLOAD`.

### Changed

- **breaking**: The response value type of Broker API `DoQuery` and `FetchResult` have incompatible changes.

### Fixed

## [0.7.0] - 2024-05-14

### Added

- Added CheckAndUpdate API for self-recovery when status is inconsistent in P2P mode.

### Fixed

- Fixed the problem that Broker was unable to detect SCQLEngine crashes or being killed by OOM.

## [0.6.0] - 2024-04-15

### Added

- Support for RSA key pairs in SCQLBroker.
- Support running on [kuscia](https://github.com/secretflow/kuscia) and scheduling SCQLEngine dynamic via kuscia job.
- Added `dry_run` parameter in DoQuery request, it could be used to check query syntax and CCL without actually executing the query.
- Improve Broker high availability, support depolying in multi-node cluster deployment.
- Support reading csv from OSS/MINIO.

### Changed

- **breaking**: Reshape column data type, data type `LONG` is deprecated.
- **breaking**: Modify table schema in broker storage for P2P mode.

## [0.5.0] - 2024-01-10

### Added

- Added support for HTTP data source router.

### Changed

- **breaking**: Add table **members** in broker storage for P2P mode.
- Speed up GROUP BY with Radix Sort.
- Adjusted configuration items for SCQLEngine and SCQLBroker.

### Fixed

- Fixed check for grant ccl in P2P mode.

## [0.4.0] - 2023-11-15

### Added

- Added support for P2P mode, no longer need to rely on a trusted third party.
- Added support for {datetime, timestamp} data types, as well as related operations.
- Support using ArrowSQL as a data source for Engine
- Added support for {Limit Cast Mod} operators.

### Changed

- Polished document outline.

## [0.3.0] - 2023-09-10

### Added

- Optimize SCQLEngine memory usage, release unused tensors immediately.
- Added warning information to the query result.
- Added support for {LEFT JOIN, RIGHT JOIN, CASE WHEN, IF} operators

### Changed

- Speed up GROUP BY with HEU in some scenarios.
- Optimized to support billion-level PSI scenarios.
- Drop GRM from SCQL awareness. We extend the syntax of create user statement and modify the syntax of create table statement.
- Used json string format to configure spu runtime in scdb yaml conf.
- Speed up JOIN, IN with Unbalanced PSI in scenarios with unbalanced data.

## [0.2.0] - 2023-06-30

### Added

- Added support for union operator.
- Added support for reading CSV files as a data source for Engine.
- Added support for using PostgreSQL as a database for Engine.
- Added support for change password with ALTER USER statement.
- Added support for removing table-level and database-level permissions with the REVOKE statement.
- Added support for structured audit log.
- Added support for the float64 data type in the Engine.
- Added the Chinese documentation.

### Changed

- Change some description in document.
- Enrich test cases.
- Enhanced support for security protocols of Cheetah and ABY3.
- Optimized GROUP BY logic.
- Optimized execution plan nodes.
- Optimized the execution logic of the runSQL.
- Optimized the three party ccl in join node.

### Fixed

- Fixed create database failed [#19](https://github.com/secretflow/scql/issues/19).
- Fixed not support group by string[#48](https://github.com/secretflow/scql/pull/48).

## [0.1.0] - 2023-03-28

### Added

- SCQL init release
