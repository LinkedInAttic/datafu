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

package datafu.test.pig.sampling;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.sampling.ReservoirSample;
import datafu.pig.sampling.SampleByKey;
import datafu.pig.sampling.WeightedSample;
import datafu.test.pig.PigTests;


public class SamplingTests extends PigTests
{
  /**
  register $JAR_PATH

  define WeightedSample datafu.pig.sampling.WeightedSample('1');
  
  data = LOAD 'input' AS (A: bag {T: tuple(v1:chararray,v2:INT)});
  
  data2 = FOREACH data GENERATE WeightedSample(A,1);
  --describe data2;
  
  STORE data2 INTO 'output';

   */
  @Multiline
  private String weightedSampleTest;
  
  @Test
  public void weightedSampleTest() throws Exception
  {
    PigTest test = createPigTestFromString(weightedSampleTest);

    writeLinesToFile("input", 
                     "({(a, 100),(b, 1),(c, 5),(d, 2)})");
                  
    test.runScript();
            
    assertOutput(test, "data2",
        "({(a,100),(c,5),(b,1),(d,2)})");
  }
  
  /**
  register $JAR_PATH

  define WeightedSample datafu.pig.sampling.WeightedSample('1');
  
  data = LOAD 'input' AS (A: bag {T: tuple(v1:chararray,v2:INT)});
  
  data2 = FOREACH data GENERATE WeightedSample(A,1,3);
  --describe data2;
  
  STORE data2 INTO 'output';
   */
  @Multiline
  private String weightedSampleLimitTest;
  
  @Test
  public void weightedSampleLimitTest() throws Exception
  {
    PigTest test = createPigTestFromString(weightedSampleLimitTest);

    writeLinesToFile("input", 
                     "({(a, 100),(b, 1),(c, 5),(d, 2)})");
                  
    test.runScript();
            
    assertOutput(test, "data2",
        "({(a,100),(c,5),(b,1)})");
  }
  
  @Test
  public void weightedSampleLimitExecTest() throws IOException
  {
    WeightedSample sampler = new WeightedSample();
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i=0; i<100; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(2);
      t.set(0, i);
      t.set(1, 1); // score is equal for all
      bag.add(t);
    }
    
    Tuple input = TupleFactory.getInstance().newTuple(3);
    input.set(0, bag);
    input.set(1, 1); // use index 1 for score
    input.set(2, 10); // get 10 items
    
    DataBag result = sampler.exec(input);
    
    Assert.assertEquals(10, result.size());
    
    // all must be found, no repeats
    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      System.out.println(i);
      Assert.assertTrue(i>=0 && i<100);
      Assert.assertFalse(String.format("Found duplicate of %d",i), found.contains(i));
      found.add(i);
    }
  }
  
  /**
  register $JAR_PATH
  
  DEFINE SampleByKey datafu.pig.sampling.SampleByKey('0.5', 'salt2.5');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FILTER data BY SampleByKey(A_id);
  
  STORE sampled INTO 'output';

   */
  @Multiline
  private String sampleByKeyTest;
  
  @Test
  public void sampleByKeyTest() throws Exception
  {
    PigTest test = createPigTestFromString(sampleByKeyTest);
    
    writeLinesToFile("input",
                     "A1\tB1\t1","A1\tB1\t4","A1\tB3\t4","A1\tB4\t4",
                     "A2\tB1\t4","A2\tB2\t4",
                     "A3\tB1\t3","A3\tB1\t1","A3\tB3\t77",
                     "A4\tB1\t3","A4\tB2\t3","A4\tB3\t59","A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3","A6\tB2\t55","A6\tB3\t1",
                     "A7\tB1\t39","A7\tB2\t27","A7\tB3\t85",
                     "A8\tB1\t4","A8\tB2\t45",
                     "A9\tB3\t92", "A9\tB1\t42","A9\tB2\t1","A9\tB3\t0",
                     "A10\tB1\t7","A10\tB2\t23","A10\tB3\t1","A10\tB4\t41","A10\tB5\t52");
    
    test.runScript();
    assertOutput(test, "sampled", 
                 "(A4,B1,3)","(A4,B2,3)","(A4,B3,59)","(A4,B4,29)",
                 "(A5,B1,4)",
                 "(A6,B2,3)","(A6,B2,55)","(A6,B3,1)",
                 "(A7,B1,39)","(A7,B2,27)","(A7,B3,85)",
                 "(A10,B1,7)","(A10,B2,23)","(A10,B3,1)","(A10,B4,41)","(A10,B5,52)");
  }

  /**
  register $JAR_PATH
  
  DEFINE SampleByKey datafu.pig.sampling.SampleByKey('0.5', 'salt2.5');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FILTER data BY SampleByKey(A_id, B_id);
  
  STORE sampled INTO 'output';

   */
  @Multiline
  private String sampleByKeyMultipleKeyTest;
  
  @Test
  public void sampleByKeyMultipleKeyTest() throws Exception
  {
    PigTest test = createPigTestFromString(sampleByKeyMultipleKeyTest);
    
    writeLinesToFile("input",
                     "A1\tB1\t1","A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3","A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3","A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92","A9\tB3\t0",
                     "A9\tB6\t42","A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23","A10\tB2\t1","A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");
    test.runScript();
    assertOutput(test, "sampled", 
                 "(A1,B1,1)","(A1,B1,4)",
                 "(A1,B4,4)",
                 "(A2,B1,4)",
                 "(A2,B2,4)",
                 "(A3,B1,3)","(A3,B1,1)",
                 "(A4,B4,29)",
                 "(A5,B1,4)",
                 "(A6,B3,1)",
                 "(A7,B1,39)",
                 "(A8,B1,4)",
                 "(A9,B3,92)","(A9,B3,0)",
                 "(A10,B2,23)","(A10,B2,1)","(A10,B2,31)"
                 );
                   
  }
  
  @Test
  public void sampleByKeyExecTest() throws Exception
  {
    SampleByKey sampler = new SampleByKey("0.10", "thesalt");
    
    Map<Integer,Integer> valuesPerKey = new HashMap<Integer,Integer>();
    
    // 10,000 keys total
    for (int i=0; i<10000; i++)
    {
      // 5 values per key
      for (int j=0; j<5; j++)
      {
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, i);
        if (sampler.exec(t))
        {          
          if (valuesPerKey.containsKey(i))
          {
            valuesPerKey.put(i, valuesPerKey.get(i)+1);
          }
          else
          {
            valuesPerKey.put(i, 1);
          }
        }
      }
    }
    
    // 10% sample, so should have roughly 1000 keys
    Assert.assertTrue(Math.abs(1000-valuesPerKey.size()) < 50);
    
    // every value should be present for the same key
    for (Map.Entry<Integer, Integer> pair : valuesPerKey.entrySet())
    {
      Assert.assertEquals(5, pair.getValue().intValue());
    }
  }
  
  /**
  register $JAR_PATH

  DEFINE ReservoirSample datafu.pig.sampling.ReservoirSample('$RESERVOIR_SIZE');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FOREACH (GROUP data ALL) GENERATE ReservoirSample(data) as sample_data;
  sampled = FOREACH sampled GENERATE COUNT(sample_data) AS sample_count;
  STORE sampled INTO 'output';

   */
  @Multiline
  private String reservoirSampleTest;
  
  @Test
  public void reservoirSampleTest() throws Exception
  {
    
    writeLinesToFile("input",
                     "A1\tB1\t1",
                     "A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3",
                     "A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3",
                     "A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92",
                     "A9\tB3\t0",
                     "A9\tB6\t42",
                     "A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23",
                     "A10\tB2\t1",
                     "A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");
   
    for(int i=10; i<=30; i=i+10){
      int reservoirSize = i ;
      PigTest test = createPigTestFromString(reservoirSampleTest, "RESERVOIR_SIZE="+reservoirSize);
      test.runScript();
      assertOutput(test, "sampled", "("+reservoirSize+")");
    }
  }
  
  @Test
  public void reservoirSampleExecTest() throws IOException
  {
    ReservoirSample sampler = new ReservoirSample("10");
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i=0; i<100; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      bag.add(t);
    }
    
    Tuple input = TupleFactory.getInstance().newTuple(bag);
    
    DataBag result = sampler.exec(input);
    
    Assert.assertEquals(10, result.size());
    
    // all must be found, no repeats
    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      System.out.println(i);
      Assert.assertTrue(i>=0 && i<100);
      Assert.assertFalse(String.format("Found duplicate of %d",i), found.contains(i));
      found.add(i);
    }
  }
  
  @Test
  public void reservoirSampleAccumulateTest() throws IOException
  {
    ReservoirSample sampler = new ReservoirSample("10");
    
    for (int i=0; i<100; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      bag.add(t);
      Tuple input = TupleFactory.getInstance().newTuple(bag);
      sampler.accumulate(input);
    }
        
    DataBag result = sampler.getValue();
    
    Assert.assertEquals(10, result.size());
    
    // all must be found, no repeats
    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      System.out.println(i);
      Assert.assertTrue(i>=0 && i<100);
      Assert.assertFalse(String.format("Found duplicate of %d",i), found.contains(i));
      found.add(i);
    }
  }
  
  @Test
  public void reservoirSampleAlgebraicTest() throws IOException
  {
    ReservoirSample.Initial initialSampler = new ReservoirSample.Initial("10");
    ReservoirSample.Intermediate intermediateSampler = new ReservoirSample.Intermediate("10");
    ReservoirSample.Final finalSampler = new ReservoirSample.Final("10");
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i=0; i<100; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(1);
      t.set(0, i);
      bag.add(t);
    }
    
    Tuple input = TupleFactory.getInstance().newTuple(bag);
    
    Tuple intermediateTuple = initialSampler.exec(input);  
    DataBag intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    intermediateTuple = intermediateSampler.exec(TupleFactory.getInstance().newTuple(intermediateBag));  
    intermediateBag = BagFactory.getInstance().newDefaultBag(Arrays.asList(intermediateTuple));
    DataBag result = finalSampler.exec(TupleFactory.getInstance().newTuple(intermediateBag));
    
    Assert.assertEquals(10, result.size());
    
    // all must be found, no repeats
    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      System.out.println(i);
      Assert.assertTrue(i>=0 && i<100);
      Assert.assertFalse(String.format("Found duplicate of %d",i), found.contains(i));
      found.add(i);
    }
  }
}
