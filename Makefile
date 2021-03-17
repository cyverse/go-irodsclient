PKG=github.com/cyverse/go-irodsclient
VERSION=v0.4.0
GIT_COMMIT?=$(shell git rev-parse HEAD)
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS?="-X ${PKG}/client.clientVersion=${VERSION} -X ${PKG}/client.gitCommit=${GIT_COMMIT} -X ${PKG}/client.buildDate=${BUILD_DATE}"
GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

#.PHONY: client
#client:
#	mkdir -p bin
#	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o bin/go-irodsclient ./cmd/

.PHONY: examples
examples:
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/list_dir/list_dir.out ./examples/list_dir/list_dir.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/upload/upload.out ./examples/upload/upload.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/download/download.out ./examples/download/download.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/delete_file/delete_file.out ./examples/delete_file/delete_file.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/delete_dir/delete_dir.out ./examples/delete_dir/delete_dir.go
	CGO_ENABLED=0 GOOS=linux go build -ldflags ${LDFLAGS} -o ./examples/make_dir/make_dir.out ./examples/make_dir/make_dir.go