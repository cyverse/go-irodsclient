#!/bin/bash

#Exit upon failure
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
export PATH=$PATH:$GOPATH/bin

if ! command -v golint &> /dev/null ; then
    go install golang.org/x/lint/golint
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


for PACKAGE in "fs" "irods" "test"
do
    PACKAGE_DIR="$SCRIPT_DIR/../$PACKAGE"
    for dir in $(go list $PACKAGE_DIR/...); do golint $dir; done | tee /tmp/output.txt
    test $(cat /tmp/output.txt | wc -l) -eq 0
    ineffassign $PACKAGE_DIR
    misspell -error $PACKAGE_DIR
done
