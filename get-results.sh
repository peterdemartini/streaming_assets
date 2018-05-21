#!/bin/bash

set -e

bytesToHuman() {
    b=${1:-0}; d=''; s=0; S=(Bytes {K,M,G,T,E,P,Y,Z}B)
    while ((b > 1024)); do
        d="$(printf "%d" $((b % 1024 * 100 / 1024)))"
        b=$((b / 1024))
        let s++
    done
    echo "$b$d ${S[$s]}"
}

get_mem_metrics() {
    local metrics=("max" "min" "mean" "median")
    local method="$1"
    local result
    local results=()
    local mem_file="./$method-kafka-mem.log"
    for metric in ${metrics[@]}; do
        result="$(datamash --format "%.f" "$metric" 1 < "$mem_file")"
        results+=("$(bytesToHuman "$result") ($metric)")
    done
    echo "[*] $method-kafka-etl CPU: ${results[*]}"
}

get_cpu_metrics() {
    local metrics=("max" "min" "mean" "median")
    local method="$1"
    local result
    local results=()
    local cpu_file="./$method-kafka-cpu.log"
    for metric in ${metrics[@]}; do
        result="$(datamash --format "%.f" "$metric" 1 < "$cpu_file")"
        results+=("$result% ($metric)")
    done
    echo "[*] $method-kafka-etl MEM: ${results[*]}"
}

main() {
    get_mem_metrics "new"
    get_mem_metrics "old"
    get_cpu_metrics "new"
    get_cpu_metrics "old"
    cat ./new-kafka-etl.log | tail -n 1
    cat ./old-kafka-etl.log | tail -n 1
}

main "$@"
