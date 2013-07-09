register $JAR_PATH

define BagUnion datafu.pig.bags.BagUnion();

data = LOAD 'input' AS (input_bag: bag {T: tuple(inner_bag: bag {T2: tuple(k: int, v: chararray)})});
describe data;

data2 = FOREACH data GENERATE BagUnion(input_bag) as unioned;
describe data2;

STORE data INTO 'output';
