register $JAR_PATH

define LevenshteinDistance datafu.pig.text.LevenshteinDistance('10');

data = load 'input' as (str1:chararray, str2:chararray);
data_out = FOREACH data GENERATE LevenshteinDistance(str1, str2) as distance;
describe data_out;
store data_out into 'output';
