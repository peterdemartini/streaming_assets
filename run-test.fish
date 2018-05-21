#!/usr/bin/env fish

function test_kafka_etl
    set -l job_name "$argv[1]-kafka-etl"
    set -l job_file "./jobs/$job_name.json"
    set -l mem_file "./$argv[1]-kafka-mem.log"
    set -l cpu_file "./$argv[1]-kafka-cpu.log"
    function gracefulExit --on-signal INT --on-signal TERM
        functions -e gracefulExit
        echo "[X] Cancelled"
        noti --title "$job_name Cancelled!"
        tjm stop "$job_file" > /dev/null
        kill %self
    end
    echo "[+] $job_name Starting -- "(date)
    tjm start "$job_file" > /dev/null; or exit 1
    set -l start_time (gdate +%s)
    set -l result 0
    set -l job_id (jq -r '.tjm.job_id' $job_file)
    set -l expected_count "5000000"
    while test "$result" != "$expected_count";
        set -l result (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$job_name" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        set -l available_count (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "fixed-data-set" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        set -l elapsed_time (math (gdate +%s) - $start_time)
        set -l worker_pid (curl -sf localhost:5678/txt/workers | grep 'worker' | grep "$job_id" | awk '{print $5}')
        if test -n "$worker_pid"
            set -l mem (ps -p "$worker_pid" -o 'rss' | tail -n 1 | xargs)
            set -l cpu (ps -p "$worker_pid" -o '%cpu' | tail -n 1 | xargs)
            echo "$mem" >> "$mem_file"
            echo "$mem" >> "$cpu_file"
            echo "[*] $job_name worker mem: $mem"
            echo "[*] $job_name worker cpu: $cpu"
        end
        echo "[*] $job_name processed: $result, available: $available_count, expected: $expected_count @ $elapsed_time seconds"
        sleep 1;
    end
    echo "[√] $job_name Done! test took $elapsed_time seconds -- "(date)
    noti --title "$job_name Done!" --message "test took $elapsed_time seconds"
    functions -e gracefulExit
    tjm stop "$job_file" > /dev/null
end

test_kafka_etl $argv
