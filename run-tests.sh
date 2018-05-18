#!/bin/bash

set -e

./before-test.fish
echo "[*] starting old test in background..."
./run-test.fish old &
echo "[*] starting new test in background..."
./run-test.fish new &
echo "[*] waiting for jobs to finish..."
wait $(jobs -p)
