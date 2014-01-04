register dist/datafu-1.2.1-SNAPSHOT.jar
register lib/packaged/opennlp-maxent-bundle-3.0.3.jar
register lib/packaged/opennlp-tools-bundle-1.5.3.jar
register lib/packaged/opennlp-uima-jar-1.5.3.jar

foo = LOAD '/Users/rjurney/Software/varaha/scripts/pos_tagging/data/ten.avro/' USING AvroStorage();
out = foreach foo GENERATE datafu.pig.text.Tokenize(text) as tokens;
dump out
