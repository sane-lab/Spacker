#!/bin/bash
# Need to substitute the default_config.py with this one.

FLINK_DIR="/home/myc/workspace/Spector/build-target"
# FLINK_DIR="/home/myc/workspace/Spector-v3/build-target"
FLINK_APP_DIR="/home/myc/workspace/flink-testbed"
SCRIPTS_DIR="/home/myc/workspace/flink-testbed/exp_scripts"
FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server"
# FLINK_CONF_DIR="${SCRIPTS_DIR}/flink-conf/conf-server-multinodes"

EXP_DIR="/data/myc/spector-proj"

# run flink clsuter
function runFlink() {
    echo "INFO: starting the cluster"
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# clean app specific related data
function cleanEnv() {
    rm -rf /tmp/flink*
    rm ${FLINK_DIR}/log/*
}


# clsoe flink clsuter
function stopFlink() {
    echo "INFO: experiment finished, stopping the cluster"
    PID=`jps | grep CliFrontend | awk '{print $1}'`
    if [[ ! -z $PID ]]; then
      kill -9 ${PID}
    fi
    ${FLINK_DIR}/bin/stop-cluster.sh
    echo "close finished"
    cleanEnv
}

# # configure parameters in flink bin
# function configFlink() {
#     # set user requirement
#     sed 's/^\(\s*spector.reconfig.affected_keys\s*:\s*\).*/\1'"$affected_keys"'/' ${FLINK_CONF_DIR}/flink-conf.yaml > tmp1
#     sed 's/^\(\s*spector.reconfig.start\s*:\s*\).*/\1'"$reconfig_start"'/' tmp1 > tmp2
#     sed 's/^\(\s*spector.reconfig.sync_keys\s*:\s*\).*/\1'"$sync_keys"'/' tmp2 > tmp3
#     sed 's/^\(\s*spector.replicate_keys_filter\s*:\s*\).*/\1'"$replicate_keys_filter"'/' tmp3 > tmp4
#     sed 's/^\(\s*controller.target.operators\s*:\s*\).*/\1'"$operator"'/' tmp4 > tmp5
#     sed 's/^\(\s*spector.reconfig.order_function\s*:\s*\).*/\1'"$order_function"'/' tmp5 > tmp6
#     sed 's/^\(\s*spector.reconfig.workload.zipf_skew\s*:\s*\).*/\1'"$zipf_skew"'/' tmp6 > tmp7
#     sed 's/^\(\s*spector.reconfig.scenario\s*:\s*\).*/\1'"$reconfig_scenario"'/' tmp7 > tmp8
#     sed 's/^\(\s*spector.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp8 > ${FLINK_CONF_DIR}/flink-conf.yaml
#     rm tmp*
#     cp ${FLINK_CONF_DIR}/* ${FLINK_DIR}/conf
# }

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*spector.reconfig.affected_keys\s*:\s*\).*/\1'"$affected_keys"'/' ${FLINK_CONF_DIR}/flink-conf.yaml > tmp1
    sed 's/^\(\s*spector.reconfig.start\s*:\s*\).*/\1'"$reconfig_start"'/' tmp1 > tmp2
    sed 's/^\(\s*spector.reconfig.sync_keys\s*:\s*\).*/\1'"$sync_keys"'/' tmp2 > tmp3
    sed 's/^\(\s*spector.replicate_keys_filter\s*:\s*\).*/\1'"$replicate_keys_filter"'/' tmp3 > tmp4
    sed 's/^\(\s*controller.target.operators\s*:\s*\).*/\1'"$operator"'/' tmp4 > tmp5
    sed 's/^\(\s*spector.reconfig.order_function\s*:\s*\).*/\1'"$order_function"'/' tmp5 > tmp6
    sed 's/^\(\s*spector.reconfig.workload.zipf_skew\s*:\s*\).*/\1'"$zipf_skew"'/' tmp6 > tmp7
    sed 's/^\(\s*spector.reconfig.scenario\s*:\s*\).*/\1'"$reconfig_scenario"'/' tmp7 > tmp8
    sed 's/^\(\s*snapshot.changelog.enabled\s*:\s*\).*/\1'"$changelog_enabled"'/' tmp8 > tmp9
    sed 's/^\(\s*policy.windowSize\s*:\s*\).*/\1'"$window_size"'/' tmp9 > tmp10
    sed 's/^\(\s*state.backend.async\s*:\s*\).*/\1'"$state_backend_async"'/' tmp10 > tmp11
    sed 's/^\(\s*spector.reconfig.affected_tasks\s*:\s*\).*/\1'"$affected_tasks"'/' tmp11 > ${FLINK_CONF_DIR}/flink-conf.yaml
    rm tmp*
    cp ${FLINK_CONF_DIR}/* ${FLINK_DIR}/conf
    # scp -r ${FLINK_CONF_DIR}/* myc@dragon:${FLINK_DIR}/conf
}

# initialization of the parameters
init() {
  # exp scenario
  reconfig_scenario="shuffle"

  # app level
  JAR="${FLINK_APP_DIR}/target/testbed-1.0-SNAPSHOT.jar"
  job="flinkapp.MicroBenchmark"
  runtime=100
  source_p=1
  per_task_rate=5000
  parallelism=8
  max_parallelism=1024
  key_set=32768
  per_key_state_size=32768 # byte
  checkpoint_interval=1000 # by default checkpoint in frequent, trigger only when necessary
  state_access_ratio=2
  order_function="default"
  zipf_skew=0

  # system level
  operator="Splitter FlatMap"
  reconfig_start=50000
  reconfig_interval=10000000
#  frequency=1 # deprecated
  affected_tasks=8
  # affected_keys=`expr ${max_parallelism} \/ ${parallelism} \/ 1` # `expr ${max_parallelism} \/ 4`
  affected_keys=`expr ${max_parallelism} \/ 2` # `expr ${max_parallelism} \/ 4`
  sync_keys=0 # disable fluid state migration
  replicate_keys_filter=0 # replicate those key%filter = 0, 1 means replicate all keys
  repeat=1
  changelog_enabled=true
  window_size=1000000000
  state_backend_async=false
}

