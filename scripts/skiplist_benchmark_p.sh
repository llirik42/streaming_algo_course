#!/bin/bash

readonly COUNT=1000000

cd /home/llirik42/GolandProjects/streaming_algo_course

for i in {1..99}; do
  p=$(awk "BEGIN {printf \"%.2f\", $i * 0.01}")
  (go run ./cmd/kvtool load -count $COUNT -store skiplist-probability -probability "$p") | ./scripts/skiplist_benchmark_p_collect.py
done

#for i in {1..19}; do
#  x=$(awk "BEGIN {printf \"%.2f\", $i * 0.05}")
#  echo "$x"
#done
