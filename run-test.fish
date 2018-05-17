set jobs_folder "./jobs"
set total_messages 1000000

function _tjm
    tjm $argv > /dev/null 2>&1 | grep -v 'ExperimentalWarning';
end

function _topic_exists
    kafka-topics --list --zookeeper localhost:2181 | grep "$argv[1]" > /dev/null;
end

function _dataset_empty
    kafka-console-consumer \
              --bootstrap-server localhost:9092 \
              --topic "$argv[1]" \
              --from-beginning \
              --timeout-ms 1000 \
              --max-messages=$total_messages > /dev/null ^| grep -v "Processed a total of $total_messages messages" 2>&1 /dev/null;
end

function _dataset_exists
    _topic_exists "$argv[1]"; and _dataset_empty "$argv[1]"
end

function test_kafka_etl
    if [ "$argv[1]" ];
        echo "* testing $argv[1]"
    else
        echo "Missing first, should be either old or new"
        exit 1
    end
    set -l job_name "$argv[1]-kafka-etl"
    echo "* stopping old-kafka-etl job"
    _tjm stop $jobs_folder/old-kafka-etl.json
    echo "* stopping new-kafka-etl job"
    _tjm stop $jobs_folder/new-kafka-etl.json
    if _dataset_exists "$argv[1]";
        echo "* deleting fix-data-set topic"
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "fixed-data-set" > /dev/null 2> /dev/null; or exit 1
        _tjm start $jobs_folder/once-data-generator-stream.json
        echo "* loading data into kafka"
        while tjm status $jobs_folder/once-data-generator-stream.json 2> /dev/null | grep -v completed > /dev/null;
            sleep 1;
        end;
        _tjm stop $jobs_folder/once-data-generator-stream.json
        echo "* done loading data into kafka"
    else
        echo "* data is already there"
    end
    echo "* deleting topic $job_name"
    if _topic_exists "$job_name";
        kafka-topics --zookeeper localhost:2181 \
            --delete \
            --topic "$job_name" > /dev/null 2> /dev/null; or exit 1
    end
    echo "* creating topic $job_name"
    kafka-topics --zookeeper localhost:2181 \
        --create \
        --topic "$job_name" \
        --partitions=1 \
        --replication-factor=1 > /dev/null 2> /dev/null; or exit 1
    echo "* resetting offset for topic $job_name"
    kafka-consumer-groups --bootstrap-server localhost:9092 \
        --topic "fixed-data-set" \
        --reset-offsets \
        --group "$job_name" \
        --to-earliest 2> /dev/null; or exit 1
    echo "* starting test - "(date)
    _tjm start "jobs/$job_name.json"
    set -l start_time (gdate +%s)
    kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$job_name" \
        --from-beginning \
        --timeout-ms 3600000 \
        --max-messages=$total_messages > "$HOME/tmp/$job_name.log"
    echo "* test complete - "(date)
    set -l end_time (gdate +%s)
    set -l result (math $end_time - $start_time)
    echo "* test took $result seconds"
    _tjm stop $jobs_folder/$job_name.json
end

test_kafka_etl "$argv[1]"
