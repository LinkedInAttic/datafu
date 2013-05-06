register $JAR_PATH

define TotalTimeBetween datafu.pig.bags.TotalTimeBetween();

data = LOAD 'input' AS (timestamp:chararray, visitor_id:chararray, url:chararray);

by_visitor = GROUP data BY visitor_id;

count_time = FOREACH by_visitor {
    ordered = ORDER data BY timestamp ASC;
    GENERATE group, TotalTimeBetween(ordered);
};

ordered_time = ORDER count_time BY $1 ASC;

STORE ordered_time INTO 'output';