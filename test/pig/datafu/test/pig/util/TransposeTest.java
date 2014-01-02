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

import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class TransposeTest extends PigTests
{
  /**
  register $JAR_PATH

  define Transpose datafu.pig.util.TransposeTupleToBag();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2:INT,val3:INT);
  
  data2 = FOREACH data GENERATE testcase, Transpose(val1 .. val3) as transposed;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, transposed;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String transposeTest;
  
  @Test
  public void transposeTest() throws Exception
  { 
    PigTest test = createPigTestFromString(transposeTest);
    writeLinesToFile("input", "1,10,11,12",
                              "2,20,21,22");
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data3");
    for (Tuple tuple : output) {
      int testCase = (Integer)tuple.get(0);
      DataBag bag = (DataBag)tuple.get(1);
      Assert.assertEquals(bag.size(), 3);
      int i=0;
      for (Tuple t : bag) {
        String expectedKey = String.format("val%d",i+1);
        Assert.assertEquals((String)t.get(0), expectedKey);
        int actualValue = (Integer)t.get(1); 
        Assert.assertEquals(actualValue, testCase*10+i);
        i++;
      }
    }
  }
  
  /**
  register $JAR_PATH

  define Transpose datafu.pig.util.TransposeTupleToBag();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2:INT,val3:DOUBLE);
  
  data2 = FOREACH data GENERATE testcase, Transpose(val1 .. val3) as transposed;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, transposed;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String transposeBadTypeTest;
  
  @Test(expectedExceptions={org.apache.pig.impl.logicalLayer.FrontendException.class})
  public void transposeBadTypeTest() throws Exception
  { 
    PigTest test = createPigTestFromString(transposeBadTypeTest);
    writeLinesToFile("input", "1,10,11,12.0",
                              "2,20,21,22.0");
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data3");
    for (Tuple tuple : output) {
      int testCase = (Integer)tuple.get(0);
      DataBag bag = (DataBag)tuple.get(1);
      Assert.assertEquals(bag.size(), 3);
      int i=0;
      for (Tuple t : bag) {
        String expectedKey = String.format("val%d",i+1);
        Assert.assertEquals((String)t.get(0), expectedKey);
        int actualValue = (Integer)t.get(1); 
        Assert.assertEquals(actualValue, testCase*10+i);
        i++;
      }
    }
  }
}
