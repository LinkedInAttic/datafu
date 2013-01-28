REGISTER piggybank.jar;
REGISTER datafu-0.0.6.jar;
REGISTER guava-13.0.1.jar; -- needed by StreamingQuantile
 
DEFINE UnixToISO   org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
DEFINE Sessionize  datafu.pig.sessions.Sessionize('10m');
DEFINE Median      datafu.pig.stats.Median();
DEFINE Quantile    datafu.pig.stats.StreamingQuantile('0.75','0.90','0.95');
DEFINE VAR         datafu.pig.stats.VAR();
 
pv = LOAD 'clicks.csv' USING PigStorage(',') AS (memberId:int, time:long, url:chararray);
 
pv = FOREACH pv
     -- Sessionize expects an ISO string
     GENERATE UnixToISO(time) as isoTime,
              time,
              memberId;
 
pv_sessionized = FOREACH (GROUP pv BY memberId) {
  ordered = ORDER pv BY isoTime;
  GENERATE FLATTEN(Sessionize(ordered)) AS (isoTime, time, memberId, sessionId);
};
 
pv_sessionized = FOREACH pv_sessionized GENERATE sessionId, time;
 
-- compute length of each session in minutes
session_times = FOREACH (GROUP pv_sessionized BY sessionId)
                GENERATE group as sessionId,
                         (MAX(pv_sessionized.time)-MIN(pv_sessionized.time))
                            / 1000.0 / 60.0 as session_length;
 
-- compute stats on session length
session_stats = FOREACH (GROUP session_times ALL) {
  ordered = ORDER session_times BY session_length;
  GENERATE
    AVG(ordered.session_length) as avg_session,
    SQRT(VAR(ordered.session_length)) as std_dev_session,
    Median(ordered.session_length) as median_session,
    Quantile(ordered.session_length) as quantiles_session;
};
 
DUMP session_stats
--(15.737532575757575,31.29552045993877,(2.848041666666667),(14.648516666666666,31.88788333333333,86.69525))