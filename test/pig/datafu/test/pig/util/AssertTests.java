package datafu.test.pig.util;

import static org.testng.Assert.*;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class AssertTests extends PigTests
{
  @Test
  public void shouldAssertWithMessageOnZero() throws Exception
  {
    try
    {
      PigTest test = createPigTest("test/pig/datafu/test/pig/util/assertWithMessageTest.pig");
      
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
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/assertWithMessageTest.pig");
    
    this.writeLinesToFile("input", "1");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data2");
  }
  
  @Test
  public void shouldAssertWithoutMessageOnZero() throws Exception
  {
    try
    {
      PigTest test = createPigTest("test/pig/datafu/test/pig/util/assertWithoutMessageTest.pig");
      
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
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/assertWithoutMessageTest.pig");
    
    this.writeLinesToFile("input", "1");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data2");
  }
}
