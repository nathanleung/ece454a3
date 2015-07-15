#!/bin/sh
clear
#! hdfs dfs -put part3.txt /user/sai/input
export CLASSPATH=$(hadoop classpath)
javac Part2.java
jar cf wc.jar Part2*.class
export HADOOP_CLASSPATH=$(pwd)/wc.jar
hdfs dfs -rm -r -skipTrash /user/sai/output
hadoop jar wc.jar Part2 /user/sai/input/ /user/sai/output/
hdfs dfs -cat /user/sai/output/*
