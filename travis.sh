#!/usr/bin/env bash

set -o errexit

./test.sh
./coverage.sh --coveralls
