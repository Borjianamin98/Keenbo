#!/usr/bin/env bash
export BASEDIR=$(dirname "$0")
export BASEDIR=$BASEDIR/..
export APP_NAME="shuffler-1.0.jar"
export JMX_PORT="9072"
export PROMETHEUS_PORT="9108"
export JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=localhost"

cd $BASEDIR

jar uf lib/$APP_NAME -C conf/ .
mkdir -p logs
cd logs

java $JAVA_OPTS -javaagent:../lib/jmx_prometheus.jar=$PROMETHEUS_PORT:../conf/jmx-promethues.yaml -jar ../lib/$APP_NAME