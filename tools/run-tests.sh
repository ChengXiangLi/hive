#!/usr/bin/env bash
export MAVEN_OPTS="-Xms1536m -Xmx1536m -XX:PermSize=1024m -XX:MaxPermSize=2048m"
mvn clean install -DskipTests -Phadoop-2
cd itests
mvn clean test -DTestSparkCliDriver -Phadoop-2
