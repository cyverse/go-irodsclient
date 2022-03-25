GO111MODULE=on
GOPROXY=direct
GOPATH=$(shell go env GOPATH)

.EXPORT_ALL_VARIABLES:

.PHONY: lint
lint:
	./tools/lint.sh

.PHONY: format
format:
	./tools/format.sh

.PHONY: examples
examples:
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/list_dir/list_dir.out ./examples/list_dir/list_dir.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/upload/upload.out ./examples/upload/upload.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/upload_parallel/upload_parallel.out ./examples/upload_parallel/upload_parallel.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/upload_parallel_async/upload_parallel_async.out ./examples/upload_parallel_async/upload_parallel_async.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/download/download.out ./examples/download/download.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/download_parallel/download_parallel.out ./examples/download_parallel/download_parallel.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/download_parallel_async/download_parallel_async.out ./examples/download_parallel_async/download_parallel_async.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/delete_file/delete_file.out ./examples/delete_file/delete_file.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/delete_dir/delete_dir.out ./examples/delete_dir/delete_dir.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/make_dir/make_dir.out ./examples/make_dir/make_dir.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/get_ticket/get_ticket.out ./examples/get_ticket/get_ticket.go
	CGO_ENABLED=0 GOOS=linux go build -o ./examples/get_ticket_anon/get_ticket_anon.out ./examples/get_ticket_anon/get_ticket_anon.go

.PHONY: test
test:
	go test -timeout 3000s -v -p 1 ./...
