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

package datafu.test.pig.stats;

import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;
import static org.testng.Assert.*;

public class EstimationTests extends PigTests
{
  /**
  register $JAR_PATH
  
  define HyperLogLogPlusPlus datafu.pig.stats.HyperLogLogPlusPlus();
  
  data_in = LOAD 'input' as (val:int);
    
  data_out = FOREACH (GROUP data_in ALL) GENERATE
    HyperLogLogPlusPlus(data_in) as cardinality;
  
  data_out = FOREACH data_out GENERATE cardinality;
    
  STORE data_out into 'output';
   */
  @Multiline private String hyperLogLogTest;
  
  @Test
  public void hyperLogLogTest() throws Exception
  {
    PigTest test = createPigTestFromString(hyperLogLogTest);

    int count = 1000000;
    String[] input = new String[count];
    for (int i=0; i<count; i++)
    {
      input[i] = Integer.toString(i*10);
    }
    
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    double error = Math.abs(count-((Long)output.get(0).get(0)))/(double)count;
    System.out.println("error: " + error*100.0 + "%");
    assertTrue(error < 0.01);
  }
}
