register $JAR_PATH

DEFINE SampleByKey datafu.pig.sampling.SampleByKey('salt2.5', '0.5');

data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
sampled = FILTER data BY SampleByKey(A_id);

grped_data = GROUP data BY A_id;
grped_data = FOREACH grped_data GENERATE group AS A_id, COUNT(data) AS count; 

grped_sampled = GROUP sampled BY A_id;
grped_sampled = FOREACH grped_sampled GENERATE group AS A_id, COUNT(sampled) AS count; 

joined = JOIN grped_data BY A_id, grped_sampled BY A_id;
joined = FILTER joined BY grped_data::count != grped_sampled::count;

STORE joined INTO 'output';
