#!/bin/bash


DEBUG=1 # 1 is true, 0 is false

function debug() {
  local str=$1
  if [ "$DEBUG" -eq 1 ]; then
    echo $str
  fi
}

function main() {
    local results_dir="results" # TODO: provide as arg
    local diff_est_dir="$results_dir/queries/PASS/DIFFERENT_ESTIMATES"
    local max_time_diff_file="$results_dir/time-experiments/max-diff"
    local class="clustertests.Test" # TODO: provide as arg
    local master=$1 # TODO: provide as arg
    local jar=target/scala-2.12/CardinalityEstimatorTest-assembly-0.1.0-SNAPSHOT.jar # TODO: provide as arg
    local args="$results_dir $master" # TODO: provide as arg


    debug "Submitting spark application..."
    spark-submit --class $class --master $master $jar $args




    local num_discrep=$(ls -1 $diff_est_dir | wc -l)
    local max_time_diff=$(cat $max_time_diff_file)

    echo "CE discrepancies found: $num_discrep"
    echo "Max time difference: $max_time_diff"
}

MASTER=$1
main $MASTER
