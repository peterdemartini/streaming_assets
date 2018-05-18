#!/usr/bin/env fish

set jobs_folder "./jobs"
set data_set "fixed-data-set"
set job_name "$argv[1]-kafka-etl"
set total_messages 10000000

function _setup_dataset
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
        tjm stop $jobs_folder/$job_name.json > /dev/null
        tjm stop $jobs_folder/once-data-generator-stream.json > /dev/null
        kill %self
    end
    echo "[*] ensure jobs are stopped before starting"
    tjm stop $jobs_folder/old-kafka-etl.json > /dev/null; or exit 1
    tjm stop $jobs_folder/new-kafka-etl.json > /dev/null; or exit 1
    _reset_kafka_for_job
    _setup_dataset
    echo "[+] Starting $job_name -- "(date)
    tjm start "jobs/$job_name.json" > /dev/null; or exit 1
    set -l start_time (gdate +%s)
    set -l checked 0
    set -l result 0
    while test "$result" != "$total_messages";
        set -l result (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$job_name" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        set -l elapsed_time (math (gdate +%s) - $start_time)
        set -l checked (math $checked + 1)
        if test $checked -ge 10;
            echo "[*] processed $result records @ $elapsed_time seconds"
            set -l checked 0
        end
        sleep 1;
    end
    echo "[√] Done! test took $result seconds -- "(date)
    functions -e gracefulExit
    tjm stop $jobs_folder/$job_name.json > /dev/null
    tjm stop $jobs_folder/once-data-generator-stream.json > /dev/null
end

test_kafka_etl "$job_name"
