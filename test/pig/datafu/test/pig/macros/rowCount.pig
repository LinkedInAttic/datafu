register $JAR_PATH

IMPORT '$MACROS_PATH';

data_in = LOAD 'input' as (val:int);

DUMP data_in;

data_out = row_count(data_in);

STORE data_out INTO 'output';