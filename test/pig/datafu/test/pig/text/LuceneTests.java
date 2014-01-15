/*
 * Copyright 2013 LinkedIn Corp. and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package datafu.test.pig.text;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;


public class LuceneTests extends PigTests
{
    /**
     register $JAR_PATH

     define JaroWinklerDistance datafu.pig.text.lucene.JaroWinklerDistance();

     data = LOAD 'input' AS (word1: chararray, word2:chararray);

     dump data;

     data2 = FOREACH data GENERATE JaroWinklerDistance(word1, word2) AS distance;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String jaroWinklerDistanceTest;

    @Test
    public void jaroWinklerDistanceTest() throws Exception
    {
        PigTest test = createPigTestFromString(jaroWinklerDistanceTest);

        writeLinesToFile("input",
                "senor software engineer\tsr. software engineer",
                "president\tpresidente");

        test.runScript();

        assertOutput(test, "data2",
                "(0.81569296)",
                "(0.99666667)");
    }

    /**
     register $JAR_PATH

     define LevensteinDistance datafu.pig.text.lucene.LevensteinDistance();

     data = LOAD 'input' AS (word1: chararray, word2:chararray);

     dump data;

     data2 = FOREACH data GENERATE LevensteinDistance(word1, word2) AS distance;

     dump data2;

    STORE data2 INTO 'output';
    */
    @Multiline
    private String levensteinDistanceTest;

    @Test
    public void levensteinDistanceTest() throws Exception
    {
        PigTest test = createPigTestFromString(levensteinDistanceTest);

        writeLinesToFile("input",
                "senor software engineer\tsr. software engineer",
                "president\tpresidente");

        test.runScript();

        assertOutput(test, "data2",
                "(0.82608694)",
                "(0.9)");
    }

    /**
     * register $JAR_PATH
     *
     * define NGramDistance datafu.pig.text.lucene.NGramDistance('3');
     *
     * data = LOAD 'input' AS (word1: chararray, word2:chararray);
     *
     * dump data;
     *
     * data2 = FOREACH data GENERATE NGramDistance(word1, word2) AS distance;
     *
     * dump data2;
     *
     * STORE data2 INTO 'output';
     */
    @Multiline
    private String nGramDistanceTest;

    @Test
    public void nGramDistanceTest() throws Exception {
        PigTest test = createPigTestFromString(nGramDistanceTest);

        writeLinesToFile("input",
                "senor software engineer\tsr. software engineer",
                "president\tpresidente");

        test.runScript();

        assertOutput(test, "data2",
                "(0.81884056)",
                "(0.9)");
    }
}
