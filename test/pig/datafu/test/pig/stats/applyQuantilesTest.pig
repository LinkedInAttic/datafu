register $JAR_PATH

define Quantile datafu.pig.stats.Quantile($QUANTILES);
define ApplyQuantile datafu.pig.stats.ApplyQuantile($QUANTILES);

data = LOAD 'input1' as (val:int);

data_grouped = GROUP data ALL;

quantiles = FOREACH data_grouped {
  sorted = ORDER data BY val;
  GENERATE Quantile(sorted) as quantiles;
}

/*describe data_out;*/

test_data = LOAD 'input2' as (val:double,id:int);

test_data = FOREACH test_data GENERATE TOTUPLE(val,id) as vals;

test_data = CROSS test_data, quantiles;

test_data = FOREACH test_data GENERATE FLATTEN(ApplyQuantile($0,$1)) as (qval,id);

STORE test_data INTO 'output';