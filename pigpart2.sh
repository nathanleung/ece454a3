export CLASSPATH=$(hadoop classpath):/usr/hdp/current/pig-client/pig-0.14.0.2.2.4.2-2-core-h2.jar
javac part2pig.java
r -cf part2pig.jar part2pig.class
pig -param input=/user/sai/input/* -param output=/user/sai/output part2pig.pig
