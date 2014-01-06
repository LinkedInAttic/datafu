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


public class NLPTests extends PigTests
{
    /**
     register $JAR_PATH

     define SentenceDetect datafu.pig.text.SentenceDetect();

     data = LOAD 'input' AS (text: chararray);

     dump data;

     data2 = FOREACH data GENERATE SentenceDetect(text) AS sentences;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String sentenceDetectTest;

    @Test
    public void sentenceDetectTest() throws Exception
    {
        PigTest test = createPigTestFromString(sentenceDetectTest);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence.",
                "Yet another sentence. One more just for luck.");

        test.runScript();

        assertOutput(test, "data2",
                "({(This is a sentence.),(This is another sentence.)})",
                "({(Yet another sentence.),(One more just for luck.)})");
    }

    /**
     register $JAR_PATH

     define TokenizeME datafu.pig.text.TokenizeME();

     data = LOAD 'input' AS (text: chararray);

     dump data;

     data2 = FOREACH data GENERATE TokenizeME(text) AS tokens;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String tokenizeMETest;

    @Test
    public void tokenizeMETest() throws Exception
    {
        PigTest test = createPigTestFromString(tokenizeMETest);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence.",
                "Yet another sentence. One more just for luck.");

        test.runScript();

        assertOutput(test, "data2",
                "({(This),(is),(a),(sentence),(.),(This),(is),(another),(sentence),(.)})",
                "({(Yet),(another),(sentence),(.),(One),(more),(just),(for),(luck),(.)})");
    }

    /**
     register $JAR_PATH

     define TokenizeSimple datafu.pig.text.TokenizeSimple();

     data = LOAD 'input' AS (text: chararray);

     dump data;

     data2 = FOREACH data GENERATE TokenizeSimple(text) AS tokens;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String tokenizeSimpleTest;

    @Test
    public void tokenizeSimpleTest() throws Exception
    {
        PigTest test = createPigTestFromString(tokenizeSimpleTest);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence.",
                "Yet another sentence. One more just for luck.");

        test.runScript();

        assertOutput(test, "data2",
                "({(This),(is),(a),(sentence),(.),(This),(is),(another),(sentence),(.)})",
                "({(Yet),(another),(sentence),(.),(One),(more),(just),(for),(luck),(.)})");
    }

    /**
     register $JAR_PATH

     define TokenizeME datafu.pig.text.TokenizeME();
     define POSTag datafu.pig.text.POSTag();

     data = LOAD 'input' AS (text: chararray);

     dump data;

     data2 = FOREACH data GENERATE TokenizeME(text) AS tokens;

     dump data2;

     data3 = FOREACH data2 GENERATE POSTag(tokens) as tagged;

     dump data3

     STORE data3 INTO 'output';
     */
    @Multiline
    private String POSTagTest;

    @Test
    public void POSTagTest() throws Exception
    {
        PigTest test = createPigTestFromString(POSTagTest);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence.",
                "Yet another sentence. One more just for luck.");

        test.runScript();

        assertOutput(test, "data3",
                "({(This,DT,0.9649410482478001),(is,VBZ,0.9982592902509803),(a,DT,0.9967282012835504),(sentence,NN,0.9772619256460584),(.,.,0.4391067883074289),(This,DT,0.8346710130761914),(is,VBZ,0.9928885242823617),(another,DT,0.9761159923140399),(sentence,NN,0.9964463493238542),(.,.,0.9856037689871404)})",
                "({(Yet,RB,0.7638997090011364),(another,DT,0.9657669183153523),(sentence,NN,0.989193114719676),(.,.,0.20091718589945456),(One,CD,0.9229251494813668),(more,JJR,0.9360382000551335),(just,RB,0.8646324491545225),(for,IN,0.9851765355889605),(luck,NN,0.9883408827371651),(.,.,0.9746378518791978)})");
    }
}
