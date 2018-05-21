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
    local new_results=()
    local old_results=()
    while read -r line; do
        new_results+=("$line")
    done < <(tail -n 3 ./new-kafka-etl.log)
    while read -r line; do
        old_results+=("$line")
    done < <(tail -n 3 ./old-kafka-etl.log)
    echo "${new_results[0]}"
    echo "${old_results[0]}"
    echo "${new_results[1]}"
    echo "${old_results[1]}"
    echo "${new_results[2]}"
    echo "${old_results[2]}"
}

main "$@"
