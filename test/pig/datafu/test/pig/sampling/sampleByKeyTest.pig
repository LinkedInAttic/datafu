register $JAR_PATH

DEFINE SampleByKey datafu.pig.sampling.SampleByKey('salt2.5', '0.5');

data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
sampled = FILTER data BY SampleByKey(A_id);

STORE sampled INTO 'output';
