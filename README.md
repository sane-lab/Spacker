# Spacker

## Introduction

Spacker is a unified framework designed for state migration in distributed stream processing systems (SPEs) such as Apache Flink. It enables configurable state migration strategies to accommodate varying performance goals, such as minimizing latency spikes, optimizing completion times, and reducing system overhead. Spacker separates the logical planning of state migration from the physical execution, allowing fine-grained control over state migration at a key-level granularity.

## Design Overview

Spacker decouples the state migration process into two main components:

Planning: Defines the migration strategy (key prioritization, progressiveness, and replication).
Execution: Implements the non-disruptive protocol to physically move the state, ensuring minimal disruption to ongoing data processing.

## Prerequisite

1. Python3
2. Zookeeper
3. Kafka
4. Java 1.8

## Code architecture

The source code of `Spacker` has been placed into `Spacker-on-Flink`, because Flink has network stack for us to achieve RPC among our components.

The main source code entrypoint is in `flink-runtime/spector/`.

## How to use?

1. Compile `Spacker-on-Flink` with : `mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true`.
2. Compile `experiments` with: `mvn clean package`.
3. Try Spacker with the following command: `cd Spacker-on-Flink/build-target`  and start a standalone cluster: `./bin/start-cluster.sh`.
4. Launch an example `StatefulDemo`  in  experiments folder: `./bin/flink run -c flinkapp.StatefulDemo experiments/target/testbed-1.0-SNAPSHOT.jar`

We have placed Spacker into the Flink, and uses Flink configuration tools to configure the parameters of Spacker. There are some configurations you can try in `flink-conf.yaml` to use the `Spacker-on-Flink`:

## Run scripts for experiments

In this project, we have mainly run experiments for three workloads:

1. Stock experiment
2. Nexmark experiment
3. Micro-Benchmark experiment

We have placed our scripts to run the experiments in `experiments/exp_scripts` folder, in which there are mainly three sub-folders.

- `spector_reconfig` contains scripts to run the corresponding experiments.
- `flink-conf` contains experiment cluster setup configurations.
- `analysis` contains the analysis scripts to process raw data and draw figures shown in our paper.

After configuring the local environment, every experiment support a one-click run such as using `spector_reconfig/micro-bench.sh` to reproduce micro-bench experiment.