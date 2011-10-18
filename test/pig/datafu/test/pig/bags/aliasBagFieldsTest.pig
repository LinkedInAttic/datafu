register $JAR_PATH

define AliasBagFields datafu.pig.bags.AliasBagFields('[a#alpha,b#numeric]');

data = LOAD 'input' AS (a:CHARARRAY, b:INT, c:INT);

describe data;

data2 = GROUP data ALL; 

describe data2;

data3 = FOREACH data2 GENERATE AliasBagFields(data) as data;

describe data3;

data4 = FOREACH data3 GENERATE FLATTEN(data);

describe data4;

data5 = FOREACH data4 GENERATE data::alpha, data::numeric;

describe data5;

STORE data5 INTO 'output';

