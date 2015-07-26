REGISTER Part4_2.jar;
Data = LOAD '$input' USING PigStorage(',');
Val = FOREACH Data GENERATE FLATTEN(Part4_2(TOBAG($1..)));
X = GROUP Val BY $0;
Result = FOREACH X GENERATE $0, AVG((bag{tuple(double)})Val.$1);
-- dump Result;
STORE Result INTO '$output' USING PigStorage(',');
