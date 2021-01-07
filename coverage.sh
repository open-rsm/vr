#!/usr/bin/env bash
# Usage: coverage.sh [-h|-v|-c|-H|-C]
#

source ./includes.sh

work_dir=.cover
profile="$work_dir/cover.out"
mode=count

function option() {
    echo -e "Usage: coverage.sh [-h|-v|-c|-H|-C]"
    echo -e "-h help \t get help"
    echo -e "-v version \t look version"
    echo -e "-c coveralls \t submit coverage statistics to coveralls.io"
    echo -e "-H html \t additionally create HTML report"
    echo -e "-C csv \t additionally create CSV report"
    exit 0
}

function version() {
    echo "coverage 0.0.1"
    exit 0
}

function generate_cover() {
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

function submit_to_coveralls() {
    echo "submit coverage statistics to coveralls.io"
    $GOPATH/bin/goveralls -coverprofile="$profile" -service=github || true
}

function main() {
    init

    cmd="coveralls"

    while getopts :hvcHC OPTION;
    do
        case $OPTION in
        h)
            option
            ;;
        v)
            version
            ;;
        c)
            submit_to_coveralls
            ;;
        H)
            cmd="html"
            ;;
        C)
            cmd="csv"
            ;;
        ?)
            echo "get a non option $OPTARG and OPTION is $OPTION"
            ;;
        esac
    done

    generate_cover $(go list ./...)
    show_csv_report

    case cmd in
    "html")
        show_html_report
        ;;
    "csv")
        cmd=show_csv_report
        ;;
    "coveralls")
        submit_to_coveralls
        ;;
    ?)
        echo >&2 "error: invalid option: $cmd"; exit 1
        ;;
    esac
}

# The beginning of everything ^_^
main $@