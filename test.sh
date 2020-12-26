#!/usr/bin/env bash

set -o errexit

GOMAXPROCS=1 go test -timeout 90s $(go list ./...)
GOMAXPROCS=4 go test -timeout 90s -race $(go list ./...)