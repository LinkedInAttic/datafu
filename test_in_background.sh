#!/usr/bin/env bash

echo "Starting test"
nohup sh test.sh > test.log &
sleep 1
echo "Process ID:"
cat test.pid
