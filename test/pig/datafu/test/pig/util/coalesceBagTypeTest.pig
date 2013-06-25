register $JAR_PATH

define COALESCE datafu.pig.util.COALESCE();

data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2: bag {tuple(aVal:int)});

data2 = FOREACH data GENERATE testcase, COALESCE(val1,val2) as result;

describe data2;

data3 = FOREACH data2 GENERATE testcase, result;

STORE data3 INTO 'output';

