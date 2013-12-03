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

package datafu.test.pig.sampling;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.sampling.SimpleRandomSample;
import datafu.test.pig.PigTests;

/**
 * Tests for {@link SimpleRandomSample}.
 * 
 * @author ximeng
 * 
 */
public class SimpleRandomSampleTest extends PigTests
{
  /**
   * register $JAR_PATH
   * 
   * DEFINE SRS datafu.pig.sampling.SimpleRandomSample('$SAMPLING_PROBABILITY');
   * 
   * data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
   * 
   * sampled = FOREACH (GROUP data ALL) GENERATE SRS(data) as sample_data;
   * 
   * sampled = FOREACH sampled GENERATE COUNT(sample_data) AS sample_count;
   * 
   * STORE sampled INTO 'output';
   */
  @Multiline
  private String simpleRandomSampleTest;

  @Test
  public void simpleRandomSampleTest() throws Exception
  {
    writeLinesToFile("input",
                     "A1\tB1\t1",
                     "A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3",
                     "A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3",
                     "A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92",
                     "A9\tB3\t0",
                     "A9\tB6\t42",
                     "A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23",
                     "A10\tB2\t1",
                     "A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");

    int n = 32;
    double p = 0.3;
    int s = (int) Math.ceil(p * n);
    PigTest test =
        createPigTestFromString(simpleRandomSampleTest, "SAMPLING_PROBABILITY=" + p);

    test.runScript();

    assertOutput(test, "sampled", "(" + s + ")");
  }

  /**
   * register $JAR_PATH
   * 
   * DEFINE SRS datafu.pig.sampling.SimpleRandomSample('$SAMPLING_PROBABILITY');
   * 
   * data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
   * 
   * sampled = FOREACH (GROUP data BY A_id) GENERATE group, SRS(data) as sample_data;
   * 
   * sampled = FOREACH sampled GENERATE group, COUNT(sample_data) AS sample_count;
   * 
   * sampled = ORDER sampled BY group;
   * 
   * STORE sampled INTO 'output';
   */
  @Multiline
  private String stratifiedSampleTest;

  @Test
  public void stratifiedSampleTest() throws Exception
  {
    writeLinesToFile("input",
                     "A1\tB1\t1",
                     "A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3",
                     "A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3",
                     "A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92",
                     "A9\tB3\t0",
                     "A9\tB6\t42",
                     "A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23",
                     "A10\tB2\t1",
                     "A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");

    double p = 0.5;

    PigTest test =
        createPigTestFromString(stratifiedSampleTest, "SAMPLING_PROBABILITY=" + p);
    test.runScript();
    assertOutput(test,
                 "sampled",
                 "(A1,2)",
                 "(A10,3)",
                 "(A2,1)",
                 "(A3,2)",
                 "(A4,2)",
                 "(A5,1)",
                 "(A6,2)",
                 "(A7,2)",
                 "(A8,1)",
                 "(A9,2)");
  }
}
