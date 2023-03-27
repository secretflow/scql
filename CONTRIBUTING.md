# Contributing

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project.

## Repo layout

- Please see [repo layout](REPO_LAYOUT.md).

## Style

### Go coding style

Go code follows [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)

### C++ coding style

In general, please use clang-format to format code, and follow clang-tidy tips.

Most of the code style is derived from the [Google C++ style guidelines](https://google.github.io/styleguide/cppguide.html), except:

* Exceptions are allowed and encouraged where appropriate.
* Header guards should use `#pragma once`.

### Other tips

* Git commit message should be meaningful, we suggest imperative [keywords](https://github.com/joelparkerhenderson/git_commit_message#summary-keywords).
* Developer must write unit-test (line coverage must be greater than 80%), tests should be deterministic.