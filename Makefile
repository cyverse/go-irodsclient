PKG=github.com/cyverse/go-irodsclient
VERSION=v0.1.0
GIT_COMMIT?=$(shell git rev-parse HEAD)
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS?="-X ${PKG}/pkg/client.clientVersion=${VERSION} -X ${PKG}/pkg/client.gitCommit=${GIT_COMMIT} -X ${PKG}/pkg/client.buildDate=${BUILD_DATE}"
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

#.PHONY: client
#client:
#	mkdir -p bin
#	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o bin/go-irodsclient ./cmd/

.PHONY: test
test:
	go test ./...
