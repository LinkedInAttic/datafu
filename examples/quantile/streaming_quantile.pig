REGISTER datafu-0.0.6.jar;

define Quartile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0');
 
temperature = LOAD 'temperature.txt' AS (id:chararray, temp:double);
 
temperature = GROUP temperature BY id;
 
temperature_quartiles = FOREACH temperature {
  -- sort not necessary
  GENERATE group as id, Quartile(temperature.temp) as quartiles;
}
 
DUMP temperature_quartiles