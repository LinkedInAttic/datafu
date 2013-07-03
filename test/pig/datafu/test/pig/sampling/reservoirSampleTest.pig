register $JAR_PATH

DEFINE ReservoirSample datafu.pig.sampling.ReservoirSample('$RESERVOIR_SIZE');

data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
sampled = FOREACH (GROUP data ALL) GENERATE ReservoirSample(data) as sample_data;
sampled = FOREACH sampled GENERATE COUNT(sample_data) AS sample_count;
STORE sampled INTO 'output';
