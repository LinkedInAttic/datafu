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

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.stats.QuantileUtil;
import datafu.pig.stats.StreamingQuantile;
import datafu.test.pig.PigTests;

public class QuantileTests  extends PigTests
{
  /**
  register $JAR_PATH
  
  define Quantile datafu.pig.stats.Quantile($QUANTILES);
  
  data_in = LOAD 'input' as (val:int);
  
  --describe data_in;
  
  data_out = GROUP data_in ALL;
  
  --describe data_out;
  
  data_out = FOREACH data_out {
    sorted = ORDER data_in BY val;
    GENERATE Quantile(sorted) as quantiles;
  }
  data_out = FOREACH data_out GENERATE FLATTEN(quantiles);
  
  --describe data_out;
  
  STORE data_out into 'output';
   */
  @Multiline private String quantileTest;
  
  @Test
  public void quantileTest() throws Exception
  {
    PigTest test = createPigTestFromString(quantileTest,
                                 "QUANTILES='0.0','0.25','0.5','0.75','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.5,8.0,10.0)");
  }
  
  @Test
  public void quantile2Test() throws Exception
  {
    PigTest test = createPigTestFromString(quantileTest,
                                 "QUANTILES='5'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.5,8.0,10.0)");
  }

  @Test
  public void quantile3Test() throws Exception {
    PigTest test = createPigTestFromString(quantileTest,
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
  public void quantile4aTest() throws Exception
  {
    PigTest test = createPigTestFromString(quantileTest,
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
    PigTest test = createPigTestFromString(quantileTest,
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
    PigTest test = createPigTestFromString(quantileTest,
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
    PigTest test = createPigTestFromString(quantileTest,
                                 "QUANTILES='0.0','0.111','0.222','0.333','0.444','0.555','0.666','0.777','0.888','1.0'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)");
  }
  
  /**
  register $JAR_PATH

  define Median datafu.pig.stats.Median();
  
  data_in = LOAD 'input' as (val:int);
  
  --describe data_in;
  
  data_out = GROUP data_in ALL;
  
  --describe data_out;
  
  data_out = FOREACH data_out {
    sorted = ORDER data_in BY val;
    GENERATE Median(sorted) as medians;
  }
  data_out = FOREACH data_out GENERATE FLATTEN(medians);
  
  --describe data_out;
  
  STORE data_out into 'output';
   */
  @Multiline private String medianTest;
  
  @Test
  public void medianTest() throws Exception
  {
    PigTest test = createPigTestFromString(medianTest);

    String[] input = {"4","5","6","9","10","7","8","2","3","1"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(5.5)");
  }
  
  /**
  register $JAR_PATH

  define Median datafu.pig.stats.StreamingMedian();
  
  data_in = LOAD 'input' as (val:int);
  
  --describe data_in;
  
  data_out = GROUP data_in ALL;
  
  --describe data_out;
  
  data_out = FOREACH data_out {
    GENERATE Median(data_in) as medians;
  }
  data_out = FOREACH data_out GENERATE FLATTEN(medians);
  
  --describe data_out;
  
  STORE data_out into 'output';
   */
  @Multiline private String streamingMedianTest;
  
  @Test
  public void streamingMedianTest() throws Exception
  {
    PigTest test = createPigTestFromString(streamingMedianTest);

    String[] input = {"0","4","5","6","9","10","7","8","2","3","1"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(5.0)");
  }
  
  /**
  register $JAR_PATH

  define Quantile datafu.pig.stats.StreamingQuantile($QUANTILES);
  
  data_in = LOAD 'input' as (val:int);
  
  --describe data_in;
  
  data_out = GROUP data_in ALL;
  
  --describe data_out;
  
  data_out = FOREACH data_out GENERATE Quantile(data_in.val) as quantiles;
  data_out = FOREACH data_out GENERATE FLATTEN(quantiles);
  
  --describe data_out;
  
  STORE data_out into 'output';
   */
  @Multiline private String streamingQuantileTest;

  @Test
  public void streamingQuantileTest() throws Exception {
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
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
    PigTest test = createPigTestFromString(streamingQuantileTest,
                                 "QUANTILES='4'");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,4.0,7.0,10.0)");
  }
  
  @Test
  public void streamingQuantileExecTest() throws Exception {
    StreamingQuantile quantile = new StreamingQuantile("4");
    
    DataBag bag;
    Tuple input;
    Tuple result;
    
    bag = BagFactory.getInstance().newDefaultBag();
    for (int i=1; i<=10; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      bag.add(t);
    }
    input = TupleFactory.getInstance().newTuple(bag);
    result = quantile.exec(input);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(1.0, result.get(0));
    Assert.assertEquals(4.0, result.get(1));
    Assert.assertEquals(7.0, result.get(2));
    Assert.assertEquals(10.0, result.get(3));
    
    // do twice to check cleanup works
    
    bag = BagFactory.getInstance().newDefaultBag();
    for (int i=11; i<=20; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      bag.add(t);
    }
    input = TupleFactory.getInstance().newTuple(bag);
    result = quantile.exec(input);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(11.0, result.get(0));
    Assert.assertEquals(14.0, result.get(1));
    Assert.assertEquals(17.0, result.get(2));
    Assert.assertEquals(20.0, result.get(3));
  }
  
  @Test
  public void streamingQuantileAccumulateTest() throws Exception {
    StreamingQuantile quantile = new StreamingQuantile("4");
    
    Tuple result;    
    
    for (int i=1; i<=10; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      quantile.accumulate(input);
    }
    result = quantile.getValue();
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(1.0, result.get(0));
    Assert.assertEquals(4.0, result.get(1));
    Assert.assertEquals(7.0, result.get(2));
    Assert.assertEquals(10.0, result.get(3));
    
    // do twice to check cleanup works
    quantile.cleanup();
    
    for (int i=11; i<=20; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      quantile.accumulate(input);
    }
    result = quantile.getValue();
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(11.0, result.get(0));
    Assert.assertEquals(14.0, result.get(1));
    Assert.assertEquals(17.0, result.get(2));
    Assert.assertEquals(20.0, result.get(3));
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
  
  /**
  register $JAR_PATH
  
  define Quantile datafu.pig.stats.$UDF($QUANTILES);
  
  data_in = LOAD 'input' as (val:int);
  
  --describe data_in;
  
  data_out = GROUP data_in ALL;
  
  --describe data_out;
  
  data_out = FOREACH data_out {
    sorted = ORDER data_in BY val;
    GENERATE Quantile(sorted) as quantiles;
  }
  data_out = FOREACH data_out GENERATE FLATTEN(quantiles);
  
  --describe data_out;
  
  data_out = FOREACH data_out GENERATE $EXPECTED_OUTPUT;
  
  STORE data_out into 'output';
   */
  @Multiline private String quantileSchemaTest;
  
  @Test
  public void quantileSchemaTest() throws Exception
  {
    PigTest test = createPigTestFromString(quantileSchemaTest,
                                 "UDF=Quantile",
                                 "QUANTILES='0.0','0.5','1.0'",
                                 "EXPECTED_OUTPUT=quantiles::quantile_0_0, "+
                                                 "quantiles::quantile_0_5, "+
                                                 "quantiles::quantile_1_0");

    String[] input = {"1","5","3","4","2"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.0)");
  }
  
  @Test
  public void quantileSchemaTest2() throws Exception
  {
    PigTest test = createPigTestFromString(quantileSchemaTest,
                                 "UDF=Quantile", 
                                 "QUANTILES='3'",
                                 "EXPECTED_OUTPUT=quantiles::quantile_0, "+
                                                 "quantiles::quantile_1, "+
                                                 "quantiles::quantile_2");

    String[] input = {"1","5","3","4","2"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.0)");
  }
  
  @Test
  public void quantileSchemaTest3() throws Exception
  {
    PigTest test = createPigTestFromString(quantileSchemaTest,
                                 "UDF=StreamingQuantile", 
                                 "QUANTILES='0.0','0.5','1.0'",
                                 "EXPECTED_OUTPUT=quantiles::quantile_0_0, "+
                                                 "quantiles::quantile_0_5, "+
                                                 "quantiles::quantile_1_0");

    String[] input = {"1","5","3","4","2"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.0)");
  }
  
  @Test
  public void quantileSchemaTest4() throws Exception
  {
    PigTest test = createPigTestFromString(quantileSchemaTest,
                                 "UDF=StreamingQuantile", 
                                 "QUANTILES='3'",
                                 "EXPECTED_OUTPUT=quantiles::quantile_0, "+
                                                 "quantiles::quantile_1, "+
                                                 "quantiles::quantile_2");

    String[] input = {"1","5","3","4","2"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(1.0,3.0,5.0)");
  }
}
