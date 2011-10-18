register $JAR_PATH

define Enumerate datafu.pig.bags.Enumerate('1', 'true');

data = LOAD 'input' AS (v1:INT,B: bag{T: tuple(v2:INT)});

data2 = GROUP data ALL;

data3 = FOREACH data2 GENERATE Enumerate(data);
describe data3;

data4 = FOREACH data3 GENERATE FLATTEN($0);
describe data4;

data4 = FOREACH data4 GENERATE $0 as v1, $1 as B, $2 as i;
describe data4;

STORE data4 INTO 'output';
