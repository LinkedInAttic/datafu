package datafu.test.pig.util;

import static org.testng.Assert.*;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class AssertTests extends PigTests
{
  /**
  register $JAR_PATH
  
  define ASSERT datafu.pig.util.ASSERT();
  
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
    
    this.getLinesForAlias(test, "data2");
  }
  
  /**
  register $JAR_PATH
  
  define ASSERT datafu.pig.util.ASSERT();
  
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
    
    this.getLinesForAlias(test, "data2");
  }
}
