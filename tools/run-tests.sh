#!/usr/bin/env bash

mvn clean install -DskipTests -Phadoop-2
cd itests
mvn clean test -DTestSparkCliDriver -Phadoop-2
