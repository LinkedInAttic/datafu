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

test_data = LOAD 'input2' as (val:double);

test_data = CROSS test_data, quantiles;

test_data = FOREACH test_data GENERATE ApplyQuantile((double)$0,$1);

STORE test_data INTO 'output';