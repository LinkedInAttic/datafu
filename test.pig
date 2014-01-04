register dist/datafu-1.2.1-SNAPSHOT.jar
register lib/packaged/opennlp-maxent-bundle-3.0.3.jar
register lib/packaged/opennlp-tools-bundle-1.5.3.jar
register lib/packaged/opennlp-uima-jar-1.5.3.jar

foo = LOAD 'data/ten.avro/' USING AvroStorage();
out = FOREACH foo GENERATE datafu.pig.text.Tokenize(text) AS tokens;
-- DUMP out

out2 = FOREACH foo GENERATE datafu.pig.text.SentenceDetect(text) AS sentences;
-- DUMP out2

out3 = FOREACH foo GENERATE datafu.pig.text.Tokenize(datafu.pig.text.SentenceDetect(text)) AS tokens;
DUMP out3