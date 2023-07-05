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

## [0.1.0] - 2023-03-28

### Added

 - SCQL init release

## [0.2.0] - 2023-06-30

### Changed

 - Change some description in document.
 - Enrich test cases.
 - Enhanced support for security protocols of Cheetah and ABY3.
 - Optimized GROUP BY logic.
 - Optimized execution plan nodes.
 - Optimized the execution logic of the runSQL.
 - Optimized the three party ccl in join node.

### Added

 - Added support for union operator.
 - Added support for reading CSV files as a data source for Engine.
 - Added support for using PostgreSQL as a database for Engine.
 - Added support for change password with ALTER USER statement.
 - Added support for removing table-level and database-level permissions with the REVOKE statement.
 - Added support for structured audit log.
 - Added support for the float64 data type in the Engine.
 - Added the Chinese documentation.

### Fixed

- Fixed create database failed [#19](https://github.com/secretflow/scql/issues/19).
- Fixed not support group by string[#48](https://github.com/secretflow/scql/pull/48).
