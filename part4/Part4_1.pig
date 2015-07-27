register FindMaxGeneVals.jar
sampleData = LOAD '$input' USING PigStorage(',');
maxGenes = FOREACH sampleData GENERATE FindMaxGeneVals($0..);
STORE maxGenes INTO '$output' USING PigStorage(',');