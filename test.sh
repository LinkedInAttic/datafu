#!/usr/bin/env bash

echo $$ > test.pid
ant test
rm test.pid
