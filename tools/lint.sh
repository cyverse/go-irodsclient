#!/bin/bash

#Exit upon failure
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export PATH=$PATH:$GOPATH/bin

if ! command -v golangci-lint &> /dev/null ; then
    # binary will be $(go env GOPATH)/bin/golangci-lint
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.61.0
fi

if ! command -v ineffassign &> /dev/null ; then
    go get github.com/gordonklaus/ineffassign
    go install github.com/gordonklaus/ineffassign
fi

if ! command -v misspell &> /dev/null ; then
    go get github.com/client9/misspell/cmd/misspell
    go install github.com/client9/misspell/cmd/misspell
fi

go mod tidy


for PACKAGE in "config" "examples" "fs" "irods" "test"
do
    PACKAGE_DIR="$SCRIPT_DIR/../$PACKAGE"
    golangci-lint run $PACKAGE_DIR/...
    ineffassign $PACKAGE_DIR/...
    misspell -error $PACKAGE_DIR
done
