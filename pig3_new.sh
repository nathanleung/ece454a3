#!/bin/sh
javac SampleToGene.java
jar cf SampleToGene.jar SampleToGene*.class
javac MultPair.java
jar cf MultPair.jar MultPair*.class

