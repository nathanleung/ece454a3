register MatrixMult.jar;
points1 = LOAD '$input' USING PigStorage(',');
points2 = LOAD '$input' USING PigStorage(',');
crossPoints = CROSS points1,points2;
sum = FOREACH crossPoints GENERATE MatrixMult($0..) as (c:chararray);
filtersum = FILTER sum BY SIZE(c) >1;
dump filtersum;
