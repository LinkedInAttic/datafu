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
import java.util.Arrays;
import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.junit.Assert;
import org.testng.annotations.Test;

import datafu.pig.stats.DoubleVAR;
import datafu.pig.stats.FloatVAR;
import datafu.pig.stats.IntVAR;
import datafu.pig.stats.LongVAR;
import datafu.pig.stats.VAR;
import datafu.test.pig.PigTests;

public class VARTests  extends PigTests
{
  /**
  register $JAR_PATH

  define VAR datafu.pig.stats.VAR();
  
  data_in = LOAD 'input' as (val:$VAL_TYPE);
  data_out = GROUP data_in ALL;
  data_out = FOREACH data_out GENERATE VAR(data_in.val) AS variance; 
  
  --describe data_out;
  STORE data_out into 'output';
   */
  @Multiline private String varTest;
  
  @Test
  public void varTestByteArray() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
                                           "VAL_TYPE=bytearray");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestDouble() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"1.0","2.0","3.0","4.0","10.0","5.0","6.0","7.0","8.0","9.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestFloat() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=float");

    String[] input = {"1.0","2.0","3.0","4.0","10.0","5.0","6.0","7.0","8.0","9.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestInteger() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=int");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestLong() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=long");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }

  @Test
  public void varTestOneData() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"5.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(0.0)");
  }
  
  
  @Test
  public void varTestZeroData() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=long");

    String[] input = {};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),0);
  }
  
  @Test
  public void varTestNullEntry() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"1","2","3","4","10","5","6","7","8","9","null"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestNullEntries() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=float");

    String[] input = {"1","2","3","4","10","5","6","7","8","9","null","null"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varExecTest() throws Exception {
    DoubleVAR var = new DoubleVAR();
    
    DataBag bag;
    Tuple input;
    Double result;
    
    bag = BagFactory.getInstance().newDefaultBag();
    for (int i=1; i<=1000; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      bag.add(t);
    }
    
    input = TupleFactory.getInstance().newTuple(1);
    input.set(0, bag);
    
    result = var.exec(input);
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
    
    // do it again to check cleanup
        
    bag = BagFactory.getInstance().newDefaultBag();
    for (int i=1; i<=2000; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      bag.add(t);
    }
    
    input = TupleFactory.getInstance().newTuple(1);
    input.set(0, bag);
    
    result = var.exec(input);
    Assert.assertTrue("Expected about 333333.2 but found " + result,Math.abs(333333.2 - result) < 1);
  }
  
  @Test
  public void varAccumulateTest() throws Exception {
    DoubleVAR var = new DoubleVAR();
    
    Double result;   
    
    for (int i=1; i<=1000; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(1);
      input.set(0, bag);
      var.accumulate(input);
    }
    
    result = var.getValue();
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
    
    // do it again to check cleanup
    var.cleanup();
        
    for (int i=1; i<=2000; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(1);
      input.set(0, bag);
      var.accumulate(input);
    }
    
    result = var.getValue();
    Assert.assertTrue("Expected about 333333.2 but found " + result,Math.abs(333333.2 - result) < 1);
  }
  
  // make sure intermediate works, where initial just passes through a single tuple, and intermediate receives a large bag of the resulting tuples
  @Test
  public void varDoubleAlgebraicIntermediateTest() throws Exception {
    DoubleVAR.Initial initialVar = new DoubleVAR.Initial();
    DoubleVAR.Intermediate intermediateVar = new DoubleVAR.Intermediate();
    DoubleVAR.Final finalVar = new DoubleVAR.Final();
    
    
    DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      intermediateBag.add(intermediateTuple);
    }
           
    Tuple intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));  
    intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure final works, where initial just passes through a single tuple, intermediate does the same, and final receives the remainder
  @Test
  public void varDoubleAlgebraicFinalTest() throws Exception {
    DoubleVAR.Initial initialVar = new DoubleVAR.Initial();
    DoubleVAR.Intermediate intermediateVar = new DoubleVAR.Intermediate();
    DoubleVAR.Final finalVar = new DoubleVAR.Final();
    
    DataBag finalBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (double)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
      intermediateBag.add(intermediateTuple);
      intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag)); 
      finalBag.add(intermediateTuple);
    }
     
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(finalBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure intermediate works, where initial just passes through a single tuple, and intermediate receives a large bag of the resulting tuples
  @Test
  public void varFloatAlgebraicIntermediateTest() throws Exception {
    FloatVAR.Initial initialVar = new FloatVAR.Initial();
    FloatVAR.Intermediate intermediateVar = new FloatVAR.Intermediate();
    FloatVAR.Final finalVar = new FloatVAR.Final();
    
    
    DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (float)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      intermediateBag.add(intermediateTuple);
    }
           
    Tuple intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));  
    intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure final works, where initial just passes through a single tuple, intermediate does the same, and final receives the remainder
  @Test
  public void varFloatAlgebraicFinalTest() throws Exception {
    FloatVAR.Initial initialVar = new FloatVAR.Initial();
    FloatVAR.Intermediate intermediateVar = new FloatVAR.Intermediate();
    FloatVAR.Final finalVar = new FloatVAR.Final();
    
    DataBag finalBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (float)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
      intermediateBag.add(intermediateTuple);
      intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag)); 
      finalBag.add(intermediateTuple);
    }
     
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(finalBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure intermediate works, where initial just passes through a single tuple, and intermediate receives a large bag of the resulting tuples
  @Test
  public void varIntAlgebraicIntermediateTest() throws Exception {
    IntVAR.Initial initialVar = new IntVAR.Initial();
    IntVAR.Intermediate intermediateVar = new IntVAR.Intermediate();
    IntVAR.Final finalVar = new IntVAR.Final();
    
    
    DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (int)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      intermediateBag.add(intermediateTuple);
    }
           
    Tuple intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));  
    intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure final works, where initial just passes through a single tuple, intermediate does the same, and final receives the remainder
  @Test
  public void varIntAlgebraicFinalTest() throws Exception {
    IntVAR.Initial initialVar = new IntVAR.Initial();
    IntVAR.Intermediate intermediateVar = new IntVAR.Intermediate();
    IntVAR.Final finalVar = new IntVAR.Final();
    
    DataBag finalBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (int)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
      intermediateBag.add(intermediateTuple);
      intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag)); 
      finalBag.add(intermediateTuple);
    }
     
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(finalBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure intermediate works, where initial just passes through a single tuple, and intermediate receives a large bag of the resulting tuples
  @Test
  public void varLongAlgebraicIntermediateTest() throws Exception {
    LongVAR.Initial initialVar = new LongVAR.Initial();
    LongVAR.Intermediate intermediateVar = new LongVAR.Intermediate();
    LongVAR.Final finalVar = new LongVAR.Final();
    
    
    DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (long)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      intermediateBag.add(intermediateTuple);
    }
           
    Tuple intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));  
    intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(intermediateBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
  
  // make sure final works, where initial just passes through a single tuple, intermediate does the same, and final receives the remainder
  @Test
  public void varLongAlgebraicFinalTest() throws Exception {
    LongVAR.Initial initialVar = new LongVAR.Initial();
    LongVAR.Intermediate intermediateVar = new LongVAR.Intermediate();
    LongVAR.Final finalVar = new LongVAR.Final();
    
    DataBag finalBag = BagFactory.getInstance().newDefaultBag();
    
    for (int i=1; i<=1000; i++)
    {
      DataBag bag;
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, (long)i);
      bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      Tuple intermediateTuple = initialVar.exec(input);
      DataBag intermediateBag = BagFactory.getInstance().newDefaultBag();
      intermediateBag.add(intermediateTuple);
      intermediateTuple = intermediateVar.exec(TupleFactory.getInstance().newTuple(intermediateBag)); 
      finalBag.add(intermediateTuple);
    }
     
    Double result = finalVar.exec(TupleFactory.getInstance().newTuple(finalBag));
    
    Assert.assertTrue("Expected about 83333.25 but found " + result,Math.abs(83333.25 - result) < 0.0001);
  }
 }
