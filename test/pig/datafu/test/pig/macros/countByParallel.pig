register $JAR_PATH

IMPORT '$MACROS_PATH';

data_in = LOAD 'input' as (key:chararray,value:int);

data_out = count_by_p(data_in,key,5); -- parallel 5

STORE data_out INTO 'output';