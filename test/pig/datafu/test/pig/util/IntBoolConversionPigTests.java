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

package datafu.test.pig.util;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class IntBoolConversionPigTests extends PigTests
{
  /**
  register $JAR_PATH
  
  define IntToBool datafu.pig.util.IntToBool();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FOREACH data GENERATE IntToBool(val);
  
  STORE data2 INTO 'output';
  */
  @Multiline private static String intToBoolTest;
  
  @Test
  public void intToBoolTest() throws Exception
  {
    PigTest test = createPigTestFromString(intToBoolTest);
        
    String[] input = {
      "", // null
      "0",
      "1"
    };
    
    String[] output = {
        "(false)",
        "(false)",
        "(true)"
      };
    
    test.assertOutput("data",input,"data2",output);
  }
  
  /**
  register $JAR_PATH
  
  define IntToBool datafu.pig.util.IntToBool();
  define BoolToInt datafu.pig.util.BoolToInt();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FOREACH data GENERATE IntToBool(val) as val;
  data3 = FOREACH data2 GENERATE BoolToInt(val) as val;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String intToBoolToIntTest;
  
  @Test
  public void intToBoolToIntTest() throws Exception
  {
    PigTest test = createPigTestFromString(intToBoolToIntTest);
        
    String[] input = {
      "", // null
      "0",
      "1",
      "2",
      "-1",
      "-2",
      "0",
      ""
    };
    
    String[] output = {
        "(0)",
        "(0)",
        "(1)",
        "(1)",
        "(1)",
        "(1)",
        "(0)",
        "(0)"
      };
    
    test.assertOutput("data",input,"data3",output);
  }
}
