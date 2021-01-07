#!/usr/bin/env bash

source ./includes.sh

function all_test() {
    GOMAXPROCS=1 go test -timeout 90s $(go list ./...)
}

function all_test_race() {
    GOMAXPROCS=4 go test -timeout 90s -race $(go list ./...)
}

function main() {
    init
    all_test
    all_test_race
}

# let's starting ^0^
main $@