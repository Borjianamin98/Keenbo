#!/usr/bin/env bash
export BASEDIR=$(dirname "$0")
export BASEDIR=$BASEDIR/..
export JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9070 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=localhost"
export APP_NAME="crawler-1.0.jar"
cd $BASEDIR

jar uf lib/$APP_NAME -C conf/ .
mkdir -p logs
cd logs

java $JAVA_OPTS -javaagent:../lib/jmx_prometheus.jar=9101:../conf/jmx-promethues.yaml -jar ../lib/$APP_NAME