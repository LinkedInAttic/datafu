register $JAR_PATH

define BagDistinct datafu.pig.bags.BagDistinct();

data = LOAD 'input' AS (B: bag {T: tuple(v:INT)});

data2 = FOREACH data GENERATE BagDistinct(B);

dump data2

STORE data2 INTO 'output';