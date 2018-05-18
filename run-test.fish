#!/usr/bin/env fish

set jobs_folder "./jobs"

function test_kafka_etl
    set -l job_name "$argv[1]-kafka-etl"
    function gracefulExit --on-signal INT --on-signal TERM
        functions -e gracefulExit
        echo "[X] Cancelled"
        noti --title "$job_name Cancelled!"
        tjm stop $jobs_folder/$job_name.json > /dev/null
        kill %self
    end
    echo "[+] $job_name Starting -- "(date)
    echo "[+] $job_name Starting -- "(date) > ./$job_name.log
    tjm start "jobs/$job_name.json" > /dev/null; or exit 1
    set -l start_time (gdate +%s)
    set -l result 0
    while test "$result" != "10000000";
        set -l result (kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$job_name" --time -1 --offsets 1 | \
            awk -F ":" '{sum += $3} END {print sum}')
        set -l elapsed_time (math (gdate +%s) - $start_time)
        echo "[*] $job_name processed $result records @ $elapsed_time seconds" >> ./$job_name.log
        sleep 1;
    end
    echo "[√] $job_name Done! test took $elapsed_time seconds -- "(date)
    echo "[√] $job_name Done! test took $elapsed_time seconds -- "(date) >> ./$job_name.log
    noti --title "$job_name Done!" --message "test took $elapsed_time seconds"
    functions -e gracefulExit
    tjm stop $jobs_folder/$job_name.json > /dev/null
end

test_kafka_etl $argv
