#!/usr/bin/env bash
echo $JAVA_HOME
ZOOKEEPER_BINARIES="/home/alon/kafka/zookeeper/apache-zookeeper-bin/bin"
KAFKA_BINARIES="/home/alon/kafka/kafka_2.12-2.6.0/bin"
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M" #avoid memory allocation problems
cd ${ZOOKEEPER_BINARIES}
./zkServer.sh start
cd ${KAFKA_BINARIES}
./kafka-server-start.sh ../config/server.properties
