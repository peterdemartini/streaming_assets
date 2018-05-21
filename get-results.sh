#!/bin/bash

set -e

bytesToHuman() {
    b=${1:-0}; d=''; s=0; S=(Bytes {K,M,G,T,E,P,Y,Z}B)
    while ((b > 1024)); do
        d="$(printf ".%02d" $((b % 1024 * 100 / 1024)))"
        b=$((b / 1024))
        let s++
    done
    echo "$b$d ${S[$s]}"
}

main() {
    local methods=("new" "old")
    local types=("cpu" "mem")
    local metrics=("max" "min" "mean")
    for method in ${methods[@]}; do
        for type in ${types[@]}; do
            local file="./$method-kafka-$type.log"
            for metric in ${metrics[@]}; do
                local result
                result="$(datamash --format "%.f" "$metric" 1 < "$file")"
                if [ "$type" == 'mem' ]; then
                    echo "$method $type $metric $(bytesToHuman "$result")"
                else
                    echo "$method $type $metric $result"
                fi
            done
        done
    done
}

main "$@"
