register $JAR_PATH

define WeightedSample datafu.pig.bags.WeightedSample();

data = LOAD 'input' AS (A: bag {T: tuple(v1:chararray,v2:INT)});

data2 = FOREACH data GENERATE WeightedSample(A);
describe data2;

STORE data2 INTO 'output';
