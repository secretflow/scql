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

## Build

### Prerequisite

#### Docker

```sh
## start dev container
docker run -d -it --name scql-dev-$(whoami) \
         --mount type=bind,source="$(pwd)",target=/home/admin/dev/ \
         -w /home/admin/dev \
         --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
         --cap-add=NET_ADMIN \
         --privileged=true \
         secretflow/scql-ci:latest /bin/bash

# attach to dev container
docker exec -it scql-dev-$(whoami) bash
```

### Build & UnitTest




```sh
# prerequisite
# spu needs numpy
pip install numpy

# build SCQL engine as release
bazel build //engine/exe:scqlengine -c opt

# test

# run unittests for SCQL engine
bazel test //engine/...

# build scdb code
make

# run scdb unit tests
go test ./pkg/...
```

### Build docs

```sh
# prerequisite
pip3 install -U -r docs/requirements.txt

# Build HTML docs, and the result is placed in directory 'docs/_build/html'
# Build documentation in English
make doc

# Build documentation in Chinese
make doc-cn
```