register $JAR_PATH

define SetUnion datafu.pig.bags.sets.SetUnion();

data = LOAD 'input' AS (B1:bag{T:tuple(val1:int,val2:int)},B2:bag{T:tuple(val1:int,val2:int)});

dump data

data2 = FOREACH data GENERATE SetUnion(B1,B2) AS C;
data2 = FOREACH data2 {
  C = ORDER C BY val1 ASC, val2 ASC;
  generate C;
}

dump data2

STORE data2 INTO 'output';
