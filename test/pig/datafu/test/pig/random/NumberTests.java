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

package datafu.test.pig.random;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class NumberTests extends PigTests
{
  /**
  register $JAR_PATH

  define RandInt datafu.pig.random.RandInt();
  
  data = LOAD 'input' AS (key:INT);
  data2 = FOREACH data GENERATE key, RandInt($MIN,$MAX) as val;
  
  STORE data2 INTO 'output';

   */
  @Multiline private String randomIntRangeTest;
  
  /**
   * Test the RandomIntRange UDF.  The main purpose is to make sure it can be used in a Pig script.
   * Also the range of output values is tested.
   * 
   * @throws Exception
   */
  @Test
  public void randomIntRangeTest() throws Exception
  {
    PigTest test = createPigTestFromString(randomIntRangeTest,
                                 "MIN=1", "MAX=10");
        
    List<String> input = new ArrayList<String>();
    for (int i=0; i<100; i++)
    {
      input.add(String.format("(%d)", i));
    }
    
    writeLinesToFile("input", 
                     input.toArray(new String[0]));
            
    test.runScript();
        
    List<Tuple> tuples = getLinesForAlias(test, "data2", false);
    for (Tuple tuple : tuples)
    {
      Integer randValue = (Integer)tuple.get(1);
      assertTrue(randValue >= 1);
      assertTrue(randValue <= 10);
    }
  }
}
