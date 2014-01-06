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

import static org.testng.Assert.assertEquals;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.bags.CountEach;
import datafu.pig.bags.DistinctBy;
import datafu.pig.bags.Enumerate;
import datafu.test.pig.PigTests;


public class NLPTests extends PigTests
{
    /**
     register $JAR_PATH

     define SentenceDetect datafu.pig.text.SentenceDetect();

     data = LOAD 'input' AS (text: chararray);

     dump data;

     data2 = FOREACH data GENERATE datafu.pig.text.SentenceDetect(text) AS sentences;

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
                "(This is a sentence. This is another sentence.)",
                "(Yet another sentence. One more just for luck.)");

        test.runScript();

        assertOutput(test, "data2",
                "({(This is a sentence.),(This is another sentence.)})",
                "({(Yet another sentence.),(One more just for luck.)})");
    }
}
