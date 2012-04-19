package datafu.test.pig.stats;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class QuantileTests  extends PigTests
{
  @Test
  public void quantileTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='0.0','0.25','0.5','0.75','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.5,8.0,10.0)");
  }
  
  @Test 
  public void applyQuantilesTest() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/applyQuantilesTest.pig",
                                 "QUANTILES='0.0','0.25','0.5','0.75','1.0'");

    // should yield quantiles (1.0,3.0,5.5,8.0,10.0)
    
    String[] input1 = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input1", input1);
    
    String[] input2 = {"0.9", "1.0", "1.1","2.0","3.0","4.0","5.0","5.49", "5.5", "5.51", "6.0","7.0","8.0","9.0","9.99","10.0","10.1"};
    writeLinesToFile("input2", input2);
    
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "test_data", true);
    
    String[] expected = {"(0.0)", "(0.0)", "(0.0)", "(0.0)", "(0.25)", "(0.25)", "(0.25)", "(0.25)", "(0.5)", "(0.5)", "(0.5)", "(0.5)", "(0.75)", "(0.75)", "(0.75)", "(1.0)", "(1.0)"};
    
    assertEquals(output.size(),expected.length);
    for (int i=0; i<expected.length; i++)
    {
      assertEquals(output.get(i).toString(), expected[i]);
    }
  }
  
  @Test
  public void quantile2Test() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='5'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.5,8.0,10.0)");
  }
  
  @Test
  public void medianTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/medianTest.pig");

    String[] input = {"4","5","6","9","10","7","8","2","3","1"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(5.5)");
  }
  
  @Test
  public void streamingMedianTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingMedianTest.pig");

    String[] input = {"0","4","5","6","9","10","7","8","2","3","1"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(5.0)");
  }

  @Test
  public void streamingQuantileTest() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='5'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.0,8.0,10.0)");
  }
  
  @Test
  public void streamingQuantile2Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='0.5','0.75','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(5.0,8.0,10.0)");
  }
  
  @Test
  public void streamingQuantile3Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='0.07','0.03','0.37','1.0','0.0'");

    List<String> input = new ArrayList<String>();
    for (int i=1000; i>=1; i--)
    {
      input.add(Integer.toString(i));
    }
    
    writeLinesToFile("input", input.toArray(new String[0]));
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(70.0,30.0,370.0,1000.0,1.0)");
  }
  
  @Test
  public void streamingQuantile4Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='0.0013','0.0228','0.1587','0.5','0.8413','0.9772','0.9987'");

    List<String> input = new ArrayList<String>();
    for (int i=100000; i>=0; i--)
    {
      input.add(Integer.toString(i));
    }
    
    writeLinesToFile("input", input.toArray(new String[0]));
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(130.0,2280.0,15870.0,50000.0,84130.0,97720.0,99870.0)");
  }
  

  
  @Test
  public void quantile3Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='0.0013','0.0228','0.1587','0.5','0.8413','0.9772','0.9987'");

    List<String> input = new ArrayList<String>();
    for (int i=100000; i>=0; i--)
    {
      input.add(Integer.toString(i));
    }
    
    writeLinesToFile("input", input.toArray(new String[0]));
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(130.0,2280.0,15870.0,50000.0,84130.0,97720.0,99870.0)");
  }
}
