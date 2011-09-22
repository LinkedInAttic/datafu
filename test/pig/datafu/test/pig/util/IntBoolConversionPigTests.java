package datafu.test.pig.util;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class IntBoolConversionPigTests extends PigTests
{
  @Test
  public void intToBoolTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/intToBoolTest.pig");
        
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
  
  @Test
  public void intToBoolToIntTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/intToBoolToIntTest.pig");
        
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
