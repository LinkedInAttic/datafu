register $JAR_PATH

define COALESCE datafu.pig.util.COALESCE();

data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);

data2 = FOREACH data GENERATE testcase, COALESCE(val1,100L) as result;

describe data2;

data3 = FOREACH data2 GENERATE testcase, result;

data4 = FOREACH data3 GENERATE testcase, result*100 as result;

STORE data4 INTO 'output';

