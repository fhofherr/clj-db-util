# Change Log

All notable changes to this project will be documented in this file.
This change log follows the conventions of
[keepachangelog.com](http://keepachangelog.com/). This project adheres
to [Semantic Versioning](http://semver.org/).

## [Unreleased][unreleased]

:exclamation: **Removes all code implemented for version
[v0.1.0](https://github.com/fhofherr/simple/compare/v0.1.0)**

### Added

* Connect to a PostgreSQL or H2 database using
  [HikariCP](http://brettwooldridge.github.io/HikariCP/) as a connection
  pool.
* Apply database migrations using [Flyway](http://flywaydb.org/).
* Monad-like abstraction of database transactions.
* Named parameters, e.g. `:param-name`.
* Load statements from classpath resources.

## 0.1.0 - 2016-01-16

Initial version of `clj-db-util`. Suffered from bad structuring and
focus issues. Released just for historical purposes.

**Don't use it**

[unreleased]: https://github.com/fhofherr/clj-db-util/compare/v0.1.0...develop
