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
            echo "[*] old test is done. Will kill new test in 30 seconds"
            for i in {1..30}; do
              ps -p "$new_pid" 2> /dev/null > /dev/null
              if [ "$?" != "0" ]; then
                break
              fi
              sleep 1
            done
            break
        fi
        if [ "$new_done" != "0" ]; then
            echo "[*] new test is done. Will kill old test in 30 seconds"
            for i in {1..30}; do
              ps -p "$old_pid" 2> /dev/null > /dev/null
              if [ "$?" != "0" ]; then
                break
              fi
              sleep 1
            done
            break
        fi
    done
    kill -INT $(jobs -p) 2>/dev/null
    ./get-results.sh
}

main "$@"
