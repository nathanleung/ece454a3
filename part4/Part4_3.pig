register MatrixMultAndSum.jar;
points1 = LOAD '$input' USING PigStorage(',');
points2 = LOAD '$input' USING PigStorage(',');
bag1 = FOREACH points1 GENERATE STRSPLIT($0,'_',2) AS sPair:tuple(n:chararray,v:int),(bag{tuple()}) TOBAG($1 ..) AS genes:bag{t:tuple()};
bag2 = FOREACH points2 GENERATE STRSPLIT($0,'_',2) AS sPair:tuple(n:chararray,v:int),(bag{tuple()}) TOBAG($1 ..) AS genes:bag{t:tuple()};
crossPoints = CROSS bag1,bag2;
crossPointsFiltered = FILTER crossPoints BY $0.v < $2.v ;
sum = FOREACH crossPointsFiltered GENERATE MatrixMultAndSum($0..);
STORE sum INTO '$output' USING PigStorage(',');