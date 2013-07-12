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
  
  define ASSERT datafu.pig.util.Assert();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FILTER data BY ASSERT(val,'assertion appears to have failed, doh!');
  
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
  
  define ASSERT datafu.pig.util.Assert();
  
  data = LOAD 'input' AS (val:INT);
  
  data2 = FILTER data BY ASSERT(val);
  
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
