#!/bin/bash
export SPARK_HOME=/memex/tandon/memoSpark
RUNNER=$JAVA_HOME/bin/java
CLASSPATH=/memex/tandon/memoSpark/conf:/memex/tandon/memoSpark/assembly/target/scala-2.10/spark-assembly-0.9.0-incubating-hadoop1.0.4.jar:/memex/tandon/memoSpark/inc_page_rank/target/scala-2.10/page-rank_2.10-1.0.jar
JAVA_OPTS=-Xmx2048m
exec "$RUNNER" -cp "$CLASSPATH" $JAVA_OPTS "$@"
