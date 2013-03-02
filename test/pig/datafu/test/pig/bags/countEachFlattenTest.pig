register $JAR_PATH

define CountEach datafu.pig.bags.CountEach('flatten');

data = LOAD 'input' AS (data: bag {T: tuple(v1:chararray)});

data2 = FOREACH data GENERATE CountEach(data) as counted;
/*describe data2;*/

data3 = FOREACH data2 {
  ordered = ORDER counted BY count DESC;
  GENERATE ordered;
}
/*describe data3*/

STORE data3 INTO 'output';
