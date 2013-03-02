register $JAR_PATH

define VAR datafu.pig.stats.VAR();

data_in = LOAD 'input' as (val:int);
data_out = GROUP data_in ALL;
data_out = FOREACH data_out GENERATE VAR(data_in.val) AS variance; 

/*describe data_out;*/
STORE data_out into 'output';
