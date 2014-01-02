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

import static org.testng.Assert.*;

import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class AssertTests extends PigTests
{
  /**
  register $JAR_PATH
  
  define ASRT datafu.pig.util.AssertUDF();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FILTER data BY ASRT(val,'assertion appears to have failed, doh!');
  
  STORE data2 INTO 'output';
  */
  @Multiline private static String assertWithMessage;
  
  @Test
  public void shouldAssertWithMessageOnZero() throws Exception
  {
    try
    {
      PigTest test = createPigTestFromString(assertWithMessage);
      
      this.writeLinesToFile("input", "0");
      
      test.runScript();
      
      this.getLinesForAlias(test, "data2");
      
      fail("test should have failed, but it didn't");
    }
    catch (Exception e)
    {
    }
  }
  
  @Test
  public void shouldNotAssertWithMessageOnOne() throws Exception
  {
    PigTest test = createPigTestFromString(assertWithMessage);
    
    this.writeLinesToFile("input", "1");
    
    test.runScript();
    
    List<Tuple> result = this.getLinesForAlias(test, "data2");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).size(), 1);
    Assert.assertEquals(result.get(0).get(0), 1);
  }
  
  /**
  register $JAR_PATH
  
  define ASRT datafu.pig.util.AssertUDF();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FILTER data BY ASRT(val);
  
  STORE data2 INTO 'output';
  */
  @Multiline private static String assertWithoutMessage;
  
  @Test
  public void shouldAssertWithoutMessageOnZero() throws Exception
  {
    try
    {
      PigTest test = createPigTestFromString(assertWithoutMessage);
      
      this.writeLinesToFile("input", "0");
      
      test.runScript();
      
      this.getLinesForAlias(test, "data2");
      
      fail("test should have failed, but it didn't");
    }
    catch (Exception e)
    {
    }
  }
  
  @Test
  public void shouldNotAssertWithoutMessageOnOne() throws Exception
  {
    PigTest test = createPigTestFromString(assertWithoutMessage);
    
    this.writeLinesToFile("input", "1");
    
    test.runScript();
    
    List<Tuple> result = this.getLinesForAlias(test, "data2");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).size(), 1);
    Assert.assertEquals(result.get(0).get(0), 1);
  }
}
