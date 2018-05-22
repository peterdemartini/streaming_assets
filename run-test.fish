#!/usr/bin/env fish

function test_kafka_etl
    set -l job_name "$argv[1]-kafka-etl"
    set -l job_file "./jobs/$job_name.json"
    set -l mem_file "./$argv[1]-kafka-mem.log"
    set -l cpu_file "./$argv[1]-kafka-cpu.log"
    tjm start "$job_file" > /dev/null; or exit 1
    set -l start_time 0
    set -l result 0
    set -l job_id (jq -r '.tjm.job_id' $job_file)
    set -l expected_count (jq -r '.operations[0].size' ./jobs/once-data-generator-stream.json)
    set -l available_count 0
    set -l elapsed_time 0
    while [ "$result" != "$expected_count" ];
        set result (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$job_name" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        set available_count (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "fixed-data-set" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        if [ "$available_count" = "0" ];
            echo "[*] $job_name waiting to start..."
            continue
        end
        if [ "$start_time" = "0" ];
            echo "[+] $job_name started"
            set start_time (gdate +%s)
        end
        set elapsed_time (math (gdate +%s) - $start_time)
        set -l worker_pid (curl -sf localhost:5678/txt/workers | grep 'worker' | grep "$job_id" | awk '{print $5}')
        if test -n "$worker_pid"
            set mem (ps -p "$worker_pid" -o 'rss' | tail -n 1 | xargs)
            set cpu (ps -p "$worker_pid" -o '%cpu' | tail -n 1 | xargs)
            echo "$mem" >> "$mem_file"
            echo "$cpu" >> "$cpu_file"
        end
        echo "[*] $job_name processed: $result, available: $available_count, expected: $expected_count @ $elapsed_time seconds"
        sleep 1;
    end
    set -l reader_batch_size (jq -r '.operations[0].size' "$job_file")
    set -l sender_batch_size (jq -r '.operations[-1].size' "$job_file")
    echo "[√] $job_name completed $result of $available_count records"
    echo "[√] $job_name reader batch size is $reader_batch_size"
    echo "[√] $job_name sender batch size is $sender_batch_size"
    echo "[√] $job_name done in $elapsed_time seconds"
    noti --title "$job_name done!" --message "test took $elapsed_time seconds"
end

test_kafka_etl $argv
