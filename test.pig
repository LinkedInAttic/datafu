register dist/datafu-1.2.1-SNAPSHOT.jar
register lib/packaged/opennlp-maxent-bundle-3.0.3.jar
register lib/packaged/opennlp-tools-bundle-1.5.3.jar
register lib/packaged/opennlp-uima-jar-1.5.3.jar

foo = LOAD 'data/ten.avro/' USING AvroStorage();
DEFINE TokenizeME datafu.pig.text.opennlp.TokenizeME('data/en-token.bin');
DEFINE SentenceDetect datafu.pig.text.opennlp.SentenceDetect('data/en-sent.bin');
DEFINE POSTag datafu.pig.text.opennlp.POSTag('data/en-pos-maxent.bin');
DEFINE FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
DEFINE JaroWinklerDistance datafu.pig.text.lucene.JaroWinklerDistance();
DEFINE LevensteinDistance datafu.pig.text.lucene.LevensteinDistance();
DEFINE NGramDistance datafu.pig.text.lucene.NGramDistance();
DEFINE NGramTokenize datafu.pig.text.lucene.NGramTokenize();

out = FOREACH foo GENERATE TokenizeME(text) AS tokens;
DUMP out

outa = FOREACH foo GENERATE datafu.pig.text.opennlp.TokenizeSimple(text) AS tokens;
DUMP outa

outb = FOREACH foo GENERATE datafu.pig.text.opennlp.TokenizeWhitespace(text) AS tokens;
DUMP outb

out2 = FOREACH foo GENERATE SentenceDetect(text) AS sentences;
out25 = FOREACH out2 GENERATE FLATTEN(sentences) AS sentences;

out3 = FOREACH out25 GENERATE TokenizeME(sentences) AS tokens;
DUMP out3

out4 = FOREACH out3 GENERATE POSTag(tokens) AS tagged;
DUMP out4

out6 = FOREACH out25 GENERATE JaroWinklerDistance(sentences, sentences) AS score;
DUMP out6

out7 = FOREACH out25 GENERATE LevensteinDistance(sentences, sentences) AS score;
DUMP out7

out8 = FOREACH out25 GENERATE NGramDistance(sentences, sentences) AS score;
DUMP out8

out9 = FOREACH out25 GENERATE NGramTokenize(sentences);
DUMP out9
