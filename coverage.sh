#!/usr/bin/env bash
# Usage: coverage.sh [--html|--coveralls]
#
#     --html      Additionally create HTML report
#     --coveralls Push coverage statistics to coveralls.io
#

set -o errexit

work_dir=.cover
profile="$work_dir/cover.out"
mode=count

function generate_cover_data() {
    rm -rf "$work_dir"
    mkdir "$work_dir"

    for pkg in "$@"; do
        f="$work_dir/$(echo $pkg | tr / -).cover"
        go test -covermode="$mode" -coverprofile="$f" "$pkg"
    done

    echo "mode: $mode" >"$profile"
    grep -h -v "^mode:" "$work_dir"/*.cover >>"$profile"
}

function show_html_report() {
    go tool cover -html="$profile" -o="$work_dir"/coverage.html
}

function show_csv_report() {
    go tool cover -func="$profile" -o="$work_dir"/coverage.csv
}

function push_to_coveralls() {
    echo "submit coverage statistics to coveralls.io"
    # ignore failure to push - it happens
    $GOPATH/bin/goveralls -coverprofile="$profile" \
                          -service=travis-ci || true
}

generate_cover_data $(go list ./...)
show_csv_report

case "$1" in
"")
    ;;
--html)
    show_html_report ;;
--coveralls)
    push_to_coveralls ;;
*)
    echo >&2 "error: invalid option: $1"; exit 1 ;;
esac
