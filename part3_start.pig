register SampleToGene.jar
register MultPair.jar
samples = LOAD '$input' USING PigStorage(',');
genes = FOREACH samples GENERATE FLATTEN(SampleToGene($0..)) as (g:chararray,t:tuple());
genes2 = FOREACH samples GENERATE FLATTEN(SampleToGene($0..)) as (g:chararray,t:tuple());
genesPair = JOIN genes BY g, genes2 BY g;
genesPairUnique = FILTER genesPair BY $1 != $3;
genesMult = FOREACH genesPairUnique GENERATE FLATTEN(MultPair($0..)) as(s:chararray,d:double);
genesMultUnique = FILTER genesMult BY SIZE(s) > 0;
samplesToSum = GROUP genesMultUnique BY (s);
sampleSum = FOREACH samplesToSum GENERATE $0, SUM((bag{tuple(double)})genesMultUnique.d);
dump sampleSum