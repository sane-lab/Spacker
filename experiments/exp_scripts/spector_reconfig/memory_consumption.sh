#!/bin/bash

source config.sh

FLINK_DIR="/home/myc/workspace/flink-profiling"
FLINK_APP_DIR="/home/myc/workspace/flink-testbed"
SCRIPTS_DIR="/home/myc/workspace/flink-testbed/exp_scripts"
FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server-profiling"


# run applications
function runApp() {
  echo "INFO: ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio} &"
  ${FLINK_DIR}/bin/flink run -c ${job} ${JAR} \
    -runtime ${runtime} -nTuples ${n_tuples}  \-p1 ${source_p} -p2 ${parallelism} -mp2 ${max_parallelism} \
    -nKeys ${key_set} -perKeySize ${per_key_state_size} -interval ${checkpoint_interval} -stateAccessRatio ${state_access_ratio} &
}

# draw figures
function analyze() {
    mkdir -p ${EXP_DIR}/raw/
    mkdir -p ${EXP_DIR}/results/

    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    echo "INFO: dump to ${EXP_DIR}/raw/${EXP_NAME}"
    if [[ -d ${EXP_DIR}/raw/${EXP_NAME} ]]; then
        rm -rf ${EXP_DIR}/raw/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${EXP_DIR}/spector/
    mv ${EXP_DIR}/spector/ ${EXP_DIR}/raw/${EXP_NAME}
    mkdir ${EXP_DIR}/spector/
}

# run one flink demo exp, which is a word count job
run_one_exp() {
  n_tuples=`expr ${runtime} \* ${per_task_rate} \* ${parallelism} \/ ${source_p}`
  # compute n_tuples from per task rates and parallelism
  EXP_NAME=spector-${per_key_state_size}-${sync_keys}-${replicate_keys_filter}-${reconfig_scenario}

  echo "INFO: run exp ${EXP_NAME}"
  configFlink
  runFlink

  python -c 'import time; time.sleep(5)'

  runApp

  SCRIPTS_RUNTIME=`expr ${runtime} + 10`
  python -c 'import time; time.sleep('"${SCRIPTS_RUNTIME}"')'

  analyze
  stopFlink

  python -c 'import time; time.sleep(5)'
}

run_memory_consumption_profiling() {
  FLINK_DIR="/home/myc/workspace/flink-profiling"
  FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server-profiling"
  init
  reconfig_scenario="profiling"
  checkpoint_interval=1000
  replicate_keys_filter=1
  sync_keys=0
  parallelism=8
  state_access_ratio=100
  per_key_state_size=4096
  run_one_exp

  FLINK_DIR="/home/myc/workspace/flink-profiling"
  FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server-profiling"
  init
  reconfig_scenario="profiling"
  checkpoint_interval=1000
  replicate_keys_filter=0
  sync_keys=0
  parallelism=8
  state_access_ratio=100
  per_key_state_size=4096
  state_backend_async=true
  run_one_exp

  FLINK_DIR="/home/myc/workspace/flink-1.8.1"
  FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server-1.8.1"
  init
  reconfig_scenario="profiling_baseline"
  checkpoint_interval=1000
  replicate_keys_filter=0
  sync_keys=0
  parallelism=8
  state_access_ratio=100
  per_key_state_size=4096
  run_one_exp
}


perf stat -a -x, -e cycles -I1000 -o ~/cpu.csv &
PERF_PID=$!
python -c 'import time; time.sleep(5)'
run_memory_consumption_profiling
kill $PERF_PID

