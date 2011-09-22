register $JAR_PATH

define BagSplit datafu.pig.bags.BagSplit();
define Enumerate datafu.pig.bags.Enumerate('1');

data = LOAD 'input' AS (name:chararray, score:double);

data = GROUP data ALL;

data2 = FOREACH data GENERATE BagSplit(3,data) as the_bags;

describe data2

data3 = FOREACH data2 GENERATE Enumerate(the_bags) as enumerated_bags;

describe data3

data4 = FOREACH data3 GENERATE FLATTEN(enumerated_bags) as (data,i);

describe data4

data5 = FOREACH data4 GENERATE
  data as the_data,
  i as the_key;   
  
data_out = foreach data5 generate flatten(the_data), the_key;
  
describe data_out