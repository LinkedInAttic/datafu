register dist/datafu-1.2.1-SNAPSHOT.jar
register lib/packaged/opennlp-maxent-bundle-3.0.3.jar
register lib/packaged/opennlp-tools-bundle-1.5.3.jar
register lib/packaged/opennlp-uima-jar-1.5.3.jar

foo = LOAD 'data/ten.avro/' USING AvroStorage();
DEFINE TokenizeME datafu.pig.text.opennlp.TokenizeME();
out = FOREACH foo GENERATE TokenizeME(text) AS tokens;
DUMP out

outa = FOREACH foo GENERATE datafu.pig.text.opennlp.TokenizeSimple(text) AS tokens;
DUMP outa

outb = FOREACH foo GENERATE datafu.pig.text.opennlp.TokenizeWhitespace(text) AS tokens;
DUMP outb

out2 = FOREACH foo GENERATE datafu.pig.text.opennlp.SentenceDetect(text) AS sentences;
out25 = FOREACH out2 GENERATE FLATTEN(sentences) AS sentences;

out3 = FOREACH out25 GENERATE TokenizeME(sentences) AS tokens;
DUMP out3

out4 = FOREACH out3 GENERATE datafu.pig.text.opennlp.POSTag(tokens) AS tagged;
DUMP out4
