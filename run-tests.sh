#!/bin/bash

_term() {
  echo "Caught SIGTERM signal!"
  kill -TERM $(jobs -p) 2>/dev/null
}

_int() {
  echo "Caught SIGINT signal!"
  kill -INT $(jobs -p) 2>/dev/null
}

trap _term SIGTERM
trap _int SIGINT

main() {
    local old_pid new_pid tail_pid
    ./before-test.fish || exit 1
    echo "[*] starting old test in background..."
    ./run-test.fish old > ./old-kafka-etl.log &
    old_pid="$!"
    echo "[*] starting new test in background..."
    ./run-test.fish new > ./new-kafka-etl.log &
    new_pid="$!"
    tail -f new-kafka-etl.log &
    tail -f old-kafka-etl.log &
    echo "[*] waiting for tests to finish..."
    while sleep 1; do
        ps -p "$old_pid" 2> /dev/null > /dev/null
        if [ "$?" != "0" ]; then
            echo "old test is done. Will kill new test in 30 seconds"
            sleep 30
            break
        fi
        ps -p "$new_pid" 2> /dev/null > /dev/null
        if [ "$?" != "0" ]; then
            echo "new test is done. Will kill old test in 30 seconds"
            sleep 30
            break
        fi
    done
    kill -INT $(jobs -p) 2>/dev/null
    ./get-results.sh
}

main "$@"
