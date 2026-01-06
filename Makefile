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
	CGO_ENABLED=0 go build -o ./examples/list_dir/list_dir.out ./examples/list_dir/list_dir.go
	CGO_ENABLED=0 go build -o ./examples/list_acls/list_acls.out ./examples/list_acls/list_acls.go
	CGO_ENABLED=0 go build -o ./examples/list_user/list_user.out ./examples/list_user/list_user.go
	CGO_ENABLED=0 go build -o ./examples/search/search.out ./examples/search/search.go
	CGO_ENABLED=0 go build -o ./examples/upload/upload.out ./examples/upload/upload.go
	CGO_ENABLED=0 go build -o ./examples/upload_parallel/upload_parallel.out ./examples/upload_parallel/upload_parallel.go
	CGO_ENABLED=0 go build -o ./examples/download/download.out ./examples/download/download.go
	CGO_ENABLED=0 go build -o ./examples/download_resumable/download_resumable.out ./examples/download_resumable/download_resumable.go
	CGO_ENABLED=0 go build -o ./examples/download_parallel/download_parallel.out ./examples/download_parallel/download_parallel.go
	CGO_ENABLED=0 go build -o ./examples/download_parallel_resumable/download_parallel_resumable.out ./examples/download_parallel_resumable/download_parallel_resumable.go
	CGO_ENABLED=0 go build -o ./examples/delete_file/delete_file.out ./examples/delete_file/delete_file.go
	CGO_ENABLED=0 go build -o ./examples/delete_dir/delete_dir.out ./examples/delete_dir/delete_dir.go
	CGO_ENABLED=0 go build -o ./examples/make_dir/make_dir.out ./examples/make_dir/make_dir.go
	CGO_ENABLED=0 go build -o ./examples/list_dir_via_ticket/list_dir.out ./examples/list_dir_via_ticket/list_dir.go
	CGO_ENABLED=0 go build -o ./examples/list_ticket/list_ticket.out ./examples/list_ticket/list_ticket.go
	CGO_ENABLED=0 go build -o ./examples/create_ticket/create_ticket.out ./examples/create_ticket/create_ticket.go
	CGO_ENABLED=0 go build -o ./examples/get_ticket/get_ticket.out ./examples/get_ticket/get_ticket.go
	CGO_ENABLED=0 go build -o ./examples/get_ticket_anon/get_ticket_anon.out ./examples/get_ticket_anon/get_ticket_anon.go
	CGO_ENABLED=0 go build -o ./examples/version/version.out ./examples/version/version.go

.PHONY: test
test:
	LOG_LEVEL=debug go test -timeout 3000s -v -p 1 -count=1 ./...
