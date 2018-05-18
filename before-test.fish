#!/usr/bin/env fish

set jobs_folder "./jobs"
set data_set "fixed-data-set"

function _reset_dataset
    if kafka-topics --list --zookeeper localhost:2181 | grep "$data_set" > /dev/null;
        echo "[*] deleting topic $data_set"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "$data_set" > /dev/null 2> /dev/null; or exit 1
    end
    tjm start $jobs_folder/once-data-generator-stream.json > /dev/null; or exit 1
    echo "[*] loading data into kafka"
end

function _reset_kafka_for_job
    set -l job_name "$argv[1]"
    if kafka-topics --list --zookeeper localhost:2181 | grep "$job_name" > /dev/null;
        echo "[*] deleting topic $job_name"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "$job_name" > /dev/null 2> /dev/null; or exit 1
    end
    echo "[*] creating topic $job_name"
    kafka-topics --zookeeper localhost:2181 \
        --create \
        --topic "$job_name" \
        --partitions=1 \
        --replication-factor=1 > /dev/null 2> /dev/null; or exit 1
    # echo "[*] resetting offset for topic $job_name"
    # kafka-consumer-groups --bootstrap-server localhost:9092 \
    #     --topic "$data_set" \
    #     --reset-offsets \
    #     --group "$job_name" \
    #     --to-earliest \
    #     --execute 2> /dev/null; or exit 1
end

function ensure_clean_slate
    echo "[*] ensure jobs are stopped before starting"
    tjm stop $jobs_folder/once-data-generator-stream.json > /dev/null; or exit 1
    tjm stop $jobs_folder/old-kafka-etl.json > /dev/null; or exit 1
    tjm stop $jobs_folder/new-kafka-etl.json > /dev/null; or exit 1
    if test -f ./new-kafka-etl.log;
        rm ./new-kafka-etl.log
    end
    if test -f ./old-kafka-etl.log;
        rm ./old-kafka-etl.log
    end

    _reset_dataset
    _reset_kafka_for_job "old-kafka-etl"
    _reset_kafka_for_job "new-kafka-etl"
    echo "[*] done cleaning up"
end

ensure_clean_slate
