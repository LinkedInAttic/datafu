package datafu.test.pig.stats;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.stats.QuantileUtil;
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
    
    String[] input2 = {"0.9\t1", "1.0\t2", "1.1\t3","2.0\t4","3.0\t5","4.0\t6","5.0\t7","5.49\t8", "5.5\t9", "5.51\t10", "6.0\t11","7.0\t12","8.0\t13","9.0\t14","9.99\t15","10.0\t16","10.1\t17"};
    writeLinesToFile("input2", input2);
    
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "test_data", true);
    
    String[] expected = {"(0.0,1)", "(0.0,2)", "(0.0,3)", "(0.0,4)", "(0.25,5)", "(0.25,6)", "(0.25,7)", "(0.25,8)", "(0.5,9)", "(0.5,10)", "(0.5,11)", "(0.5,12)", "(0.75,13)", "(0.75,14)", "(0.75,15)", "(1.0,16)", "(1.0,17)"};
    
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
  public void quantile4aTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='4'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,4.0,7.0,10.0)");
  }
  
  @Test
  public void quantile4bTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='0.0','0.333','0.666','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,4.0,7.0,10.0)");
  }
  
  @Test
  public void quantile5aTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='10'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)");
  }
  
  @Test
  public void quantile5bTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/quantileTest.pig",
                                 "QUANTILES='0.0','0.111','0.222','0.333','0.444','0.555','0.666','0.777','0.888','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)");
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
  public void streamingQuantile5aTest() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='10'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)");
  }
  
  @Test
  public void streamingQuantile5bTest() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='0.0','0.111','0.222','0.333','0.444','0.555','0.666','0.777','0.888','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)");
  }
  
  @Test
  public void streamingQuantile6Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='0.0','0.333','0.666','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,4.0,7.0,10.0)");
  }
  
  @Test
  public void streamingQuantile7Test() throws Exception {
    PigTest test = createPigTest("test/pig/datafu/test/pig/stats/streamingQuantileTest.pig",
                                 "QUANTILES='4'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,4.0,7.0,10.0)");
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
  
  @Test
  public void quantileParamsTest() throws Exception {
    List<Double> quantiles = QuantileUtil.getQuantilesFromParams("5");
    
    assertEquals(quantiles.size(),5);
    assertAboutEqual(quantiles.get(0), 0.0);
    assertAboutEqual(quantiles.get(1), 0.25);
    assertAboutEqual(quantiles.get(2), 0.5);
    assertAboutEqual(quantiles.get(3), 0.75);
    assertAboutEqual(quantiles.get(4), 1.0);
  }
  
  @Test
  public void quantileParamsTest2() throws Exception {
    List<Double> quantiles = QuantileUtil.getQuantilesFromParams("2");
    
    assertEquals(quantiles.size(),2);
    assertAboutEqual(quantiles.get(0), 0.0);
    assertAboutEqual(quantiles.get(1), 1.0);
  }
  
  @Test
  public void quantileParamsTest3() throws Exception {
    List<Double> quantiles = QuantileUtil.getQuantilesFromParams("11");
    
    assertEquals(quantiles.size(),11);
    assertAboutEqual(quantiles.get(0), 0.0);
    assertAboutEqual(quantiles.get(1), 0.1);
    assertAboutEqual(quantiles.get(2), 0.2);
    assertAboutEqual(quantiles.get(3), 0.3);
    assertAboutEqual(quantiles.get(4), 0.4);
    assertAboutEqual(quantiles.get(5), 0.5);
    assertAboutEqual(quantiles.get(6), 0.6);
    assertAboutEqual(quantiles.get(7), 0.7);
    assertAboutEqual(quantiles.get(8), 0.8);
    assertAboutEqual(quantiles.get(9), 0.9);
    assertAboutEqual(quantiles.get(10), 1.0);
  }
  
  @Test
  public void quantileParamsTest4() throws Exception {
    List<Double> quantiles = QuantileUtil.getQuantilesFromParams("10");
    
    assertEquals(quantiles.size(),10);
    assertAboutEqual(quantiles.get(0), 0.0);
    assertAboutEqual(quantiles.get(1), 0.11111);
    assertAboutEqual(quantiles.get(2), 0.22222);
    assertAboutEqual(quantiles.get(3), 0.33333);
    assertAboutEqual(quantiles.get(4), 0.44444);
    assertAboutEqual(quantiles.get(5), 0.55555);
    assertAboutEqual(quantiles.get(6), 0.66666);
    assertAboutEqual(quantiles.get(7), 0.77777);
    assertAboutEqual(quantiles.get(8), 0.88888);
    assertAboutEqual(quantiles.get(9), 1.0);
  }
  
  @Test
  public void quantileParamsTest5() throws Exception {
    List<Double> quantiles = QuantileUtil.getQuantilesFromParams("4");
    
    assertEquals(quantiles.size(),4);
    assertAboutEqual(quantiles.get(0), 0.0);
    assertAboutEqual(quantiles.get(1), 0.333);
    assertAboutEqual(quantiles.get(2), 0.666);
    assertAboutEqual(quantiles.get(3), 1.0);
  }
  
  private void assertAboutEqual(double actual, double expected) {
    assertTrue(Math.abs(actual-expected) < 0.001);
  }
}
