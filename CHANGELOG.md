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
 - Drop GRM from SCQL awareness. We extend the syntax of  create user statement and modify the syntax of create table statement.
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