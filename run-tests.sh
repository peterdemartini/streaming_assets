#!/bin/bash

die() {
  run_command_on_jobs "stop"
  kill $(jobs -p) 2>/dev/null
}

_term() {
  echo "Caught SIGTERM signal!"
  die
}

_int() {
  echo "Caught SIGINT signal!"
  die
}

trap _term SIGTERM
trap _int SIGINT

reset_dataset() {
    if kafka-topics --list --zookeeper localhost:2181 | grep "fixed-data-set" > /dev/null; then
        echo "[*] deleting topic fixed-data-set"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "fixed-data-set" > /dev/null 2> /dev/null || return 1
    fi
    tjm start ./jobs/once-data-generator-stream.json > /dev/null || return 1
    echo "[*] loading data into kafka... sleeping for 10 seconds to get a head start"
    sleep 10
}

reset_kafka_for_job() {
    local job_name="$1"
    if kafka-topics --list --zookeeper localhost:2181 | grep "$job_name" > /dev/null; then
        echo "[*] deleting topic $job_name"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "$job_name" > /dev/null 2> /dev/null || return 1
    fi
    echo "[*] creating topic $job_name"
    kafka-topics --zookeeper localhost:2181 \
        --create \
        --topic "$job_name" \
        --partitions=1 \
        --replication-factor=1 > /dev/null 2> /dev/null || return 1
    # echo "[*] resetting offset for topic $job_name"
    # kafka-consumer-groups --bootstrap-server localhost:9092 \
    #     --topic "fixed-data-set" \
    #     --reset-offsets \
    #     --group "$job_name" \
    #     --to-earliest \
    #     --execute 2> /dev/null || return 1
}

run_command_on_jobs() {
    local cmd="$1"
    local jobs=("once-data-generator-stream" "old-kafka-etl" "new-kafka-etl")
    for job_name in "${jobs[@]}"; do
        tjm "$cmd" "./jobs/$job_name.json"
    done
}

remove_log_files() {
    local log_files=("$(ls ./*-kafka-*.log)")
    for file in "${log_files[@]}"; do
        if [ -f "./$file" ]; then
            rm "./$file"
            touch "./$file"
        fi
    done
}

ensure_clean_slate() {
    echo "[*] cleaning up"
    run_command_on_jobs "stop" && \
        run_command_on_jobs "update" && \
        remove_log_files && \
        reset_dataset && \
        reset_kafka_for_job "old-kafka-etl" && \
        reset_kafka_for_job "new-kafka-etl"
    echo "[*] done cleaning up"
}

main() {
    local old_pid new_pid
    ensure_clean_slate
    echo "[*] starting old test in background..."
    ./run-test.fish old > ./old-kafka-etl.log &
    old_pid="$!"
    echo "[*] starting new test in background..."
    ./run-test.fish new > ./new-kafka-etl.log &
    new_pid="$!"
    while sleep 1; do
        local old_done new_done
        ps -p "$old_pid" 2> /dev/null > /dev/null
        old_done="$?"
        ps -p "$new_pid" 2> /dev/null > /dev/null
        new_done="$?"
        if [ "$old_done" != "0" ] && [ "$new_done" != "0" ]; then
          echo "[*] both tests are done"
          break
        fi
        if [ "$old_done" != "0" ]; then
            echo "[*] old test is done. If new job doesn't complete in 30 seconds the test will terminate"
            for i in {1..30}; do
              if ps -p "$new_pid" 2> /dev/null > /dev/null; then
                sleep 1
              else
                echo "[*] killing new test..."
                kill "$new_pid"
                break
              fi
            done
            break
        fi
        if [ "$new_done" != "0" ]; then
            echo "[*] new test is done. If old job doesn't complete in 30 seconds the test will terminate"
            for i in {1..30}; do
              if ps -p "$old_pid" 2> /dev/null > /dev/null; then
                sleep 1
              else
                echo "[*] killing old test..."
                kill "$old_pid"
                break
              fi
              sleep 1
            done
            break
        fi
    done
    die && ./get-results.sh
}

main "$@"
