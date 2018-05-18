#!/usr/bin/env fish

set jobs_folder "./jobs"
set data_set "fixed-data-set"
set job_name "$argv[1]-kafka-etl"
set total_messages 10000000

function _tjm
    tjm $argv > /dev/null 2>&1 | grep -v 'ExperimentalWarning';
end

function _setup_dataset
    if kafka-topics --list --zookeeper localhost:2181 | grep "$data_set" > /dev/null;
        echo "[*] deleting $data_set topic"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "$data_set" > /dev/null 2> /dev/null; or exit 1
    end
    _tjm start $jobs_folder/once-data-generator-stream.json
    echo "[*] loading data into kafka"
end

function _reset_kafka_for_job
    if kafka-topics --list --zookeeper localhost:2181 | grep "$job_name" > /dev/null;;
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

function test_kafka_etl
    if [ "$job_name" ];
         echo "[+] Setting up test"
    else
        echo "Missing first, should be either old or new"
        exit 1
    end
    function gracefulExit --on-signal INT --on-signal TERM
        functions -e gracefulExit
        echo "[X] Cancelled"
        tjm stop $jobs_folder/$job_name.json 2> /dev/null > /dev/null
        tjm stop $jobs_folder/once-data-generator-stream.json 2> /dev/null > /dev/null
        kill %self
    end
    echo "[*] ensure jobs are stopped before starting"
    _tjm stop $jobs_folder/old-kafka-etl.json
    _tjm stop $jobs_folder/new-kafka-etl.json
    _reset_kafka_for_job
    _setup_dataset
    echo "[+] Starting $job_name -- "(date)
    _tjm start "jobs/$job_name.json"
    set -l start_time (gdate +%s)
    kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$job_name" \
        --from-beginning \
        --timeout-ms 3600000 \
        --max-messages=$total_messages > /dev/null
    set -l result (math (gdate +%s) - $start_time)
    echo "[√] Done! test took $result seconds -- "(date)
    functions -e gracefulExit
    tjm stop $jobs_folder/$job_name.json 2> /dev/null > /dev/null
    tjm stop $jobs_folder/once-data-generator-stream.json 2> /dev/null > /dev/null
end

test_kafka_etl "$job_name"
