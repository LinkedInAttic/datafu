register $JAR_PATH

DEFINE LogisticRegressionFunc datafu.pig.bags.LogisticRegressionFunc();

samp = load 'input' using PigStorage(',') as (label:int,feature1:double,feature2:double);
describe samp;

group_samp = group samp all;
logreg = foreach group_samp generate LogisticRegressionFunc(samp);

store logreg into 'output';
