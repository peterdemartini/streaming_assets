#!/bin/bash

set -e

bytesToHuman() {
    b=${1:-0}; s=0; S=(Bytes {K,M,G,T,E,P,Y,Z}B)
    while ((b > 1024)); do
        b=$((b / 1024))
        let s++
    done
    echo "$b ${S[$s]}"
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
    echo "[*] $method-kafka-etl MEM: ${results[*]}"
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
    echo "[*] $method-kafka-etl CPU: ${results[*]}"
}

main() {
    get_mem_metrics "new"
    get_mem_metrics "old"
    get_cpu_metrics "new"
    get_cpu_metrics "old"
    local new_results=()
    local old_results=()
    while read -r line; do
        new_results+=("$line")
    done < <(tail -n 4 ./new-kafka-etl.log)
    while read -r line; do
        old_results+=("$line")
    done < <(tail -n 4 ./old-kafka-etl.log)
    for i in {0..3}; do
        echo "${new_results[$i]}"
        echo "${old_results[$i]}"
    done
}

main "$@"
