#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-oracle/jre
export CONFIG_JDBC_USERNAME=postgres
export CONFIG_JDBC_PASSWORD=
export CONFIG_JDBC_URL=jdbc:postgresql://127.0.0.1/config

java -Xmx4g -jar -DINSTANCE_NAMES=TEST001  /opt/sftpreader/sftpreader-1.0-SNAPSHOT.jar
