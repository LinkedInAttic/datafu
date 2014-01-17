/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *           http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package datafu.test.pig.sampling;

import java.io.IOException;
import java.util.*;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.sampling.WeightedReservoirSample;
import datafu.pig.sampling.WeightedReservoirSampleWithExpJump;
import datafu.test.pig.PigTests;

/**
 * Tests for {@link WeightedReservoirSampleWithExpJumpTests}.
 *
 * @author wjian 
 *
 */
public class WeightedReservoirSampleWithExpJumpTests extends PigTests
{
 /** 
  register $JAR_PATH 
 
  define WeightedSample datafu.pig.sampling.WeightedReservoirSampleWithExpJump('1','1'); 

  data = LOAD 'input' AS (v1:chararray,v2:INT);
  data_g = group data all;
  sampled = FOREACH data_g {data_o = order data by v1 DESC; GENERATE WeightedSample(data_o);}
  --describe sampled; 
   
  STORE sampled INTO 'output'; 
 
  */ 
  @Multiline 
  private String weightedSampleAccTest; 
   
  @Test 
  public void weightedSampleAccumulateTest() throws Exception 
  {
     Map<String, Integer> count = new HashMap<String, Integer>();

     count.put("a", 0);
     count.put("b", 0);
     count.put("c", 0);
     count.put("d", 0);

     PigTest test = createPigTestFromString(weightedSampleAccTest); 
 
     writeLinesToFile("input",  
                "b\t2","c\t1","d\t5","a\t100");

     for(int i = 0; i < 10; i++) {
        test.runScript();

        List<Tuple> output = this.getLinesForAlias(test, "sampled");

        Tuple t = output.get(0);

        DataBag sampleBag = (DataBag)t.get(0);

        for(Iterator<Tuple> sampleIter = sampleBag.iterator(); sampleIter.hasNext();) {
           Tuple st = sampleIter.next();
           String key = (String)st.get(0);
           count.put(key, count.get(key) + 1);
        }              
     }

     String maxKey = "";
     int maxCount = 0;
     for(String key : count.keySet()) {
        if(maxCount < count.get(key)) {
           maxKey = key; 
           maxCount = count.get(key);
        } 
     }

     Assert.assertEquals(maxKey, "a");
  }

  @Test 
  public void weightedSampleAlgebraicTest() throws Exception 
  {
     Map<String, Integer> count = new HashMap<String, Integer>();

     count.put("a", 0);
     count.put("b", 0);
     count.put("c", 0);
     count.put("d", 0);
     count.put("e", 0);
     count.put("f", 0);
     count.put("g", 0);
     count.put("h", 0);

     Map<String, Integer> weights = new HashMap<String, Integer>();

     weights.put("a", 100);
     weights.put("b", 2);
     weights.put("c", 1);
     weights.put("d", 5);
     weights.put("e", 10);
     weights.put("f", 7);
     weights.put("g", 3);
     weights.put("h", 5);

     List<Tuple> arrTupleList = new ArrayList<Tuple>();

     for (Map.Entry<String, Integer> entry : weights.entrySet()) {
         Tuple t = TupleFactory.getInstance().newTuple(2);
         t.set(0, entry.getKey());
         t.set(1, entry.getValue());
         arrTupleList.add(t);
     }

     int numTrials = 10;
     
     int mappers = 3;

     int combiners = 2;

     for(int trial = 0; trial < numTrials; trial++) {

        Collections.shuffle(arrTupleList);
        
        DataBag bag = BagFactory.getInstance().newDefaultBag();

        int mapperRecords = new Double(Math.ceil((double)(arrTupleList.size()) / mappers)).intValue();

        List<Tuple> initialInput = new ArrayList<Tuple>();

        int j = 0;
        for (int i = 0; i < mappers; i++) {
           for (; j < arrTupleList.size() && bag.size() < mapperRecords; j++) {
              bag.add(arrTupleList.get(j));
           }
           initialInput.add(TupleFactory.getInstance().newTuple(bag));
           bag = BagFactory.getInstance().newDefaultBag();
        }
    
        DataBag sampleBag = generateWeightedSamplesUsingCombiner(initialInput, "1", "1", 2);
 
        for(Iterator<Tuple> sampleIter = sampleBag.iterator(); sampleIter.hasNext();) {
           Tuple st = sampleIter.next();
           String key = (String)st.get(0);
           count.put(key, count.get(key) + 1);
        }              
     }

     String maxKey = "";
     int maxCount = 0;
     for(String key : count.keySet()) {
        if(maxCount < count.get(key)) {
           maxKey = key; 
           maxCount = count.get(key);
        } 
     }

     Assert.assertEquals(maxKey, "a");
  }

  @Test
  public void weightedSampleAccumulateIntegerityTest() throws IOException
  {
     WeightedReservoirSampleWithExpJump sampler = new WeightedReservoirSampleWithExpJump("10", "1");

     for (int i=0; i<100; i++)
     {
       Tuple t = TupleFactory.getInstance().newTuple(2);
       t.set(0, i);
       t.set(1, i + 1);
       DataBag bag = BagFactory.getInstance().newDefaultBag();
       bag.add(t);
       Tuple input = TupleFactory.getInstance().newTuple(bag);
       sampler.accumulate(input);
     }

     DataBag result = sampler.getValue();
     verifyNoRepeatAllFound(result, 10, 0, 100); 
  }

  @Test
  public void weightedSampleAlgebraicIntegerityTest() throws IOException
  {
    List<Tuple> initialInput = new ArrayList<Tuple>();

    List<Tuple> sampleInput = new ArrayList<Tuple>();
    for (int i=0; i<100; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(2);
      t.set(0, i);
      t.set(1, i + 1);
      sampleInput.add(t);
    }

    Collections.shuffle(sampleInput);

    for (int i = 0; i < 5; i++) {
       DataBag bag = BagFactory.getInstance().newDefaultBag();
       for (int j = 0; j < 20; j++) {
          bag.add(sampleInput.get(i * 20 + j));
       }
       initialInput.add(TupleFactory.getInstance().newTuple(bag));
    }
    
    DataBag result = generateWeightedSamplesUsingCombiner(initialInput, "10", "1", 2);
    verifyNoRepeatAllFound(result, 10, 0, 100); 
  }

  private DataBag generateWeightedSamplesUsingCombiner(List<Tuple> initialInput,
                                                       String strNumSamples,
                                                       String strWeightIdx,
                                                       int combiners) throws IOException
  {
    List<Tuple> initialOutputTupleList = new ArrayList<Tuple>();
    for(Tuple initialInputTuple : initialInput) {
        WeightedReservoirSampleWithExpJump.Initial initialSampler = new WeightedReservoirSampleWithExpJump.Initial(strNumSamples, strWeightIdx);
        Tuple initialOutputTuple = initialSampler.exec(initialInputTuple);
        initialOutputTupleList.add(initialOutputTuple);
    }

    List<Tuple> finalTupleList = new ArrayList<Tuple>();

    List<Tuple> intermedTupleList = new ArrayList<Tuple>();
    for(int i = 0; i < initialOutputTupleList.size(); ) {
        for(int j = 0; j < combiners && i < initialOutputTupleList.size(); j++, i++) {
           intermedTupleList.add(initialOutputTupleList.get(i));
        }
        WeightedReservoirSampleWithExpJump.Intermediate intermediateSampler = new WeightedReservoirSampleWithExpJump.Intermediate(strNumSamples, strWeightIdx);
        DataBag finalBag = BagFactory.getInstance().newDefaultBag(intermedTupleList);
        Tuple finalTuple = intermediateSampler.exec(TupleFactory.getInstance().newTuple(finalBag));
        finalTupleList.add(finalTuple);
        intermedTupleList = new ArrayList<Tuple>();
    }

    DataBag finalBag = BagFactory.getInstance().newDefaultBag(finalTupleList);
    WeightedReservoirSampleWithExpJump.Final finalSampler = new WeightedReservoirSampleWithExpJump.Final(strNumSamples, strWeightIdx);
    DataBag result = finalSampler.exec(TupleFactory.getInstance().newTuple(finalBag));
    return result;
  }

  private void verifyNoRepeatAllFound(DataBag result,
                                      int expectedResultSize,
                                      int left,
                                      int right) throws ExecException
  {
    Assert.assertEquals(expectedResultSize, result.size());
    
    // all must be found, no repeats
    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      Assert.assertTrue(i>=left && i<right);
      Assert.assertFalse(String.format("Found duplicate of %d",i), found.contains(i));
      found.add(i);
    }
  }

  @Test
  public void weightedSampleLimitExecTest() throws IOException
  {
    WeightedReservoirSampleWithExpJump sampler = new WeightedReservoirSampleWithExpJump("100", "1");
   
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    for (int i=0; i<10; i++)
    {
      Tuple t = TupleFactory.getInstance().newTuple(2);
      t.set(0, i);
      t.set(1, 1); // score is equal for all
      bag.add(t);
    }
   
    Tuple input = TupleFactory.getInstance().newTuple(1);
    input.set(0, bag);
   
    DataBag result = sampler.exec(input);
   
    verifyNoRepeatAllFound(result, 10, 0, 10); 

    Set<Integer> found = new HashSet<Integer>();
    for (Tuple t : result)
    {
      Integer i = (Integer)t.get(0);
      found.add(i);
    }

    for(int i = 0; i < 10; i++)
    {
      Assert.assertTrue(found.contains(i));
    }
  }

  @Test
  public void invalidConstructorArgTest() throws Exception
  {
    try {
         WeightedReservoirSampleWithExpJump sampler = new WeightedReservoirSampleWithExpJump("1", "-1");
         Assert.fail( "Testcase should fail");
    } catch (Exception ex) {
         Assert.assertTrue(ex.getMessage().indexOf("Invalid negative weight field index argument in WeightedReservoirSampleWithExpJump reservoir constructor: -1") >= 0);
    }
  }

  @Test
  public void invalidWeightTest() throws Exception
  {
    PigTest test = createPigTestFromString(weightedSampleAccTest);

    writeLinesToFile("input",  
                "a\t100","b\t1","c\t0","d\t2");
    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "sampled");
         Assert.fail( "Testcase should fail");
    } catch (Exception ex) {
    }
  }

 /** 
  register $JAR_PATH 
 
  define WeightedSample datafu.pig.sampling.WeightedReservoirSampleWithExpJump('1','1'); 

  data = LOAD 'input' AS (v1:chararray);
  data_g = group data all;
  sampled = FOREACH data_g GENERATE WeightedSample(data);
  describe sampled; 
   
  STORE sampled INTO 'output'; 
 
  */ 
  @Multiline 
  private String invalidInputTupleSizeTest; 
 
  @Test
  public void invalidInputTupleSizeTest() throws Exception
  {
    PigTest test = createPigTestFromString(invalidInputTupleSizeTest);

    writeLinesToFile("input",  
                "a","b","c","d");
    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "sampled");
         Assert.fail( "Testcase should fail");
    } catch (Exception ex) {
         Assert.assertTrue(ex.getMessage().indexOf("The field schema of the input tuple is null or the tuple size is no more than the weight field index: 1") >= 0);
    }
  }

 /** 
  register $JAR_PATH 
 
  define WeightedSample datafu.pig.sampling.WeightedReservoirSampleWithExpJump('1','0'); 

  data = LOAD 'input' AS (v1:chararray, v2:INT);
  data_g = group data all;
  sampled = FOREACH data_g GENERATE WeightedSample(data);
  describe sampled; 
   
  STORE sampled INTO 'output'; 
 
  */ 
  @Multiline 
  private String invalidWeightFieldSchemaTest; 
 
  @Test
  public void invalidWeightFieldSchemaTest() throws Exception
  {
    PigTest test = createPigTestFromString(invalidWeightFieldSchemaTest);

    writeLinesToFile("input",  
                "a\t100","b\t1","c\t5","d\t2");
    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "sampled");
         Assert.fail( "Testcase should fail");
    } catch (Exception ex) {
         Assert.assertTrue(ex.getMessage().indexOf("Expect the type of the weight field of the input tuple to be of ([int, long, float, double]), but instead found (chararray), weight field: 0") >= 0);
    }
  }
}
