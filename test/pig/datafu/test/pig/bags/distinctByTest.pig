register $JAR_PATH

define DistinctBy datafu.pig.bags.DistinctBy('0');

data = LOAD 'input' AS (a:CHARARRAY, b:INT, c:INT);

describe data;

data2 = GROUP data ALL; 

describe data2;

data3 = FOREACH data2 GENERATE DistinctBy(data);

describe data3;

STORE data3 INTO 'output';

