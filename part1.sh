#!/bin/sh
jar cf Part1.jar Part1*.class
export HADOOP_CLASSPATH=$(pwd)/Part1/jar
hdfs dfs -rm -r -skipTrash /user/nhleung/output
hadoop jar Part1.jar Part1 /user/nhleung/input /user/nhleung/output
hdfs dfs -cat /user/nhleung/output/*
