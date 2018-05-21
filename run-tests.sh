#!/bin/bash

set -e

./before-test.fish
echo "[*] starting old test in background..."
./run-test.fish old > ./old-kafka-etl.log &
echo "[*] starting new test in background..."
./run-test.fish new > ./new-kafka-etl.log &
echo "[*] waiting for jobs to finish..."
wait $(jobs -p)
./get-results.sh
