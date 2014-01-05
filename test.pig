register dist/datafu-1.2.1-SNAPSHOT.jar
register lib/packaged/opennlp-maxent-bundle-3.0.3.jar
register lib/packaged/opennlp-tools-bundle-1.5.3.jar
register lib/packaged/opennlp-uima-jar-1.5.3.jar

foo = LOAD 'data/ten.avro/' USING AvroStorage();
out = FOREACH foo GENERATE datafu.pig.text.Tokenize(text) AS tokens;
-- DUMP out

out2 = FOREACH foo GENERATE FLATTEN(datafu.pig.text.SentenceDetect(text)) AS sentences:chararray;
-- DUMP out2

out3 = FOREACH out2 GENERATE datafu.pig.text.Tokenize(sentences) AS tokens;
DUMP out3

out4 = FOREACH out3 GENERATE datafu.pig.text.POSTag(tokens) AS tagged;
-- DUMP out4
