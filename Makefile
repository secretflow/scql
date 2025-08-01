export GO111MODULE=on
GOPATH := ${GOPATH}:${PWD}
TOOLBIN := ${PWD}/tool-bin
export PATH := ${TOOLBIN}:$(PATH)
export GOFLAGS=-buildmode=pie -buildvcs=false
export CGO_CPPFLAGS=-fstack-protector-strong -D_FORTIFY_SOURCE=2
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	export CGO_LDFLAGS=-Wl,-z,relro,-z,now,-z,noexecstack
endif

.PHONY: clean vet lint test detect-shadowing fast pb prepare fmt gogenerate

default: install

install: clean prepare fmt vet gogenerate
	GOBIN=${PWD}/bin go install -ldflags "-X main.version=${SCQL_VERSION}" ./cmd/...

gogenerate:
	go generate ./pkg/...
	go generate ./cmd/...

fast: fmt vet
	GOBIN=${PWD}/bin go install ./cmd/...

parser:
	cd pkg/parser && make

binary: clean prepare fmt vet gogenerate
	$(eval SCQL_VERSION := $(shell bash ${PWD}/version_build.sh))
	echo "Binary version: ${SCQL_VERSION}"
	GOBIN=${PWD}/bin go install -ldflags "-X main.version=${SCQL_VERSION}" ./cmd/...
	bazelisk --host_jvm_args=-Xmx8g build //engine/exe:scqlengine -c opt --jobs=32
	bash ${PWD}/version_build.sh -r

pb: clean
	$(RM) -rf pkg/proto-gen/*
	./api/generate_proto.sh && bash ./contrib/agent/proto/generate_proto.sh

fmt:
	go fmt ./pkg/...

vet:
	go vet -unsafeptr=false ./pkg/...

doc:
	go run ./cmd/docgen/main.go
	cd docs && rm -rf _build && make build

lint: GOLINT-exists
	-${TOOLBIN}/golangci-lint run --out-format=colored-line-number

detect-shadowing:
	go vet -vettool=$(shell which shadow) -strict ./...

clean:
	$(RM) bin/*
	$(RM) *.coverprofile

test:
	go test -v -cover ./pkg/...

testsum:
	go run gotest.tools/gotestsum@latest ./pkg/...

coverage: install
	go list -f '{{if gt (len .TestGoFiles) 0}}"go test -covermode count -coverprofile {{.Name}}.coverprofile -coverpkg ./... {{.ImportPath}}"{{end}}' ./... | xargs -I {} bash -c {}
	find . -name "*.coverprofile"
	$(info Use `go tool cover -html MODULE_NAME.coverprofile`)

prepare: GO-exists GO-package

GO-exists:
	$(if $(shell command -v go 2> /dev/null),$(info Found `go`),$(error Please install go (prefer v1.22): refer to `https://golang.org/dl/`))
	go version
	go env GOPROXY

GOLINT-exists:
	$(if $(shell command -v golangci-lint 2> /dev/null),$(info Found `golangci-lint`),$(shell curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ${TOOLBIN} v1.61.0))

GO-package:
	@GOBIN=${TOOLBIN} go install go.uber.org/mock/mockgen@latest && \
	GOBIN=${TOOLBIN} go install golang.org/x/tools/cmd/goyacc@latest && \
	GOBIN=${TOOLBIN} go install golang.org/x/tools/cmd/cover@latest && \
	GOBIN=${TOOLBIN} go install github.com/mattn/goveralls@latest && \
	GOBIN=${TOOLBIN} go install github.com/rakyll/gotest@latest && \
	GOBIN=${TOOLBIN} go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow@latest
