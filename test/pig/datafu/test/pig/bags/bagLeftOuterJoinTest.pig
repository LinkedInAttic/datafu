register $JAR_PATH

define BagLeftOuterJoin datafu.pig.bags.BagLeftOuterJoin();

data = LOAD 'input' AS (outer_key:chararray, bag1:bag{T:tuple(k:chararray,v:chararray)}, bag2:bag{T:tuple(k:chararray,v:chararray)}, bag3:bag{T:tuple(k3:chararray,v3:chararray)});
describe data;

data2 = FOREACH data GENERATE 
  outer_key, 
  BagLeftOuterJoin(bag1, 'k', bag2, 'k', bag3, 'k3') as joined1,
  BagLeftOuterJoin(bag1, 'k', bag3, 'k3', bag2, 'k') as joined2; --this will break without UDF signature and pig < 0.11
describe data2;

STORE data2 INTO 'output';
