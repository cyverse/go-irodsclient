#!/bin/bash

#Exit upon failure
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"


for PACKAGE in "fs" "irods" "test"
do
    PACKAGE_DIR="$SCRIPT_DIR/../$PACKAGE"
    gofmt -s -l -d $PACKAGE_DIR | tee /tmp/format_output.txt
    test $(cat /tmp/format_output.txt | wc -l) -eq 0
done
