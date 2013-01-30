register $JAR_PATH

IMPORT '$MACROS_PATH';

data_in = LOAD 'input' as (key:chararray,value:int);

data_out = count_by(data_in,key);

STORE data_out INTO 'output';