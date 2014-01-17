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

package datafu.test.pig.stats.entropy;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;


/*
 * Use R function to compute condition entropy as the test benchmark
 * http://cran.r-project.org/web/packages/infotheo/infotheo.pdf
 */
public class StreamingEmpiricalCondEntropyTests extends AbstractEntropyTests
{
  /**
  register $JAR_PATH

  define CondEntropy datafu.pig.stats.entropy.stream.StreamingCondEntropy();
  
  data = load 'input' as (valX:double, valY:chararray);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY *;
                     GENERATE CondEntropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String condEntropy;
  
  @Test
  public void uniqValStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(condEntropy); 
    
    writeLinesToFile("input",
                     "98.94791	click",
                     "38.61010	view",
                     "97.10575	view",
                     "62.28313	click",
                     "38.83960	click",
                     "32.05370	view",
                     "96.10962	view",
                     "28.72388	click",
                     "96.65888	view",
                     "20.41135	click");
        
    test.runScript();
   
    /*
     * library(infotheo)
     * X=c("98.94791","38.61010","97.10575","62.28313","38.83960","32.05370","96.10962","28.72388","96.65888","20.41135")
     * Y=c("click","view","view","click","click","view","view","click","view","click")
     * condentropy(Y,X)
     * [1] 0
     */ 
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test
  public void singleValStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(condEntropy);
    
    writeLinesToFile("input",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click",
                     "98.94791	click");
        
    test.runScript();

    /*
     * library(infotheo)
     * X=c("98.94791","98.94791","98.94791","98.94791","98.94791","98.94791","98.94791","98.94791","98.94791","98.94791")
     * Y=c("click","click","click","click","click","click","click","click","click","click")
     * condentropy(Y,X)
     * [1] 0
     */ 
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test
  public void dupValStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(condEntropy); 
    
    writeLinesToFile("input",
                     "98.94791	click",
                     "38.61010	click",
                     "97.10575	view",
                     "62.28313	view",
                     "38.61010	view",
                     "32.05370	view",
                     "96.10962	click",
                     "38.61010	click",
                     "96.10962	view",
                     "20.41135	click");
        
    test.runScript();
 
    /*
     * library(infotheo)
     * X=c("98.94791","38.61010","97.10575","62.28313","38.61010","32.05370","96.10962","38.61010","96.10962","20.41135")
     * Y=c("click","click","view","view","view","view","click","click","view","click")
     * condentropy(Y,X)
     * [1] 0.3295837 
     */    
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.3295837);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test
  public void emptyInputBagStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(condEntropy);
    
    writeLinesToFile("input"
                     );

    test.runScript();
    
    List<Double> expectedOutput = new ArrayList<Double>();
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test
  public void singleElemInputBagStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(condEntropy);
    
    writeLinesToFile("input",
                     "98.94791	view");

    test.runScript();
     /*
     * library(infotheo)
     * X = c("98.94791")
     * Y = c("view")
     * condentropy(Y,X)
     * [1] 0
     */      
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.stream.StreamingCondEntropy();
  
  data = load 'input' as (valX1:chararray, valX2:chararray, valY:chararray);
  data = foreach data generate (valX1, valX2) as X, valY as Y;
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY *;
                     GENERATE Entropy(data_ordered);
             };

  store data_out into 'output';
   */
  @Multiline private String pairCondEntropy;
 
  @Test
  public void dupPairValStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(pairCondEntropy);
    
    writeLinesToFile("input",
                     "hadoop	bigdata	click",
                     "hadoop	pig	view",
                     "hadoop	datafu	click",
                     "datafu	pig	click",
                     "bigdata	pig	view",
                     "datafu	pig	click",
                     "datafu	pig	view",
                     "hadoop	bigdata	view",
                     "pig	datafu	view",
                     "pig	datafu	view");
        
    test.runScript();

    /*
     * library(infotheo)
     * X=c("hadoop bigdata","hadoop pig","hadoop datafu","datafu pig","bigdata pig","datafu pig","datafu pig","hadoop bigdata","pig datafu","pig datafu")
     * Y=c("click","view","click","click","view","click","view","view","view","view")
     * condentropy(X,Y)
     * [1] 0.3295837
     */   
 
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.3295837);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5); 
  }

  /**
  register $JAR_PATH

  define CondEntropy datafu.pig.stats.entropy.stream.StreamingCondEntropy('$type','$base');
  
  data = load 'input' as (valX:double, valY:chararray);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY *;
                     GENERATE CondEntropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String condLogEntropy;
 
  @Test
  public void dupValStreamingEmpiricalCondEntropoyLog2Test() throws Exception
  {
    PigTest test = createPigTestFromString(condLogEntropy, "type=empirical", "base=log2");
 
    writeLinesToFile("input",
                     "98.94791	click",
                     "38.61010	click",
                     "97.10575	view",
                     "62.28313	view",
                     "38.61010	view",
                     "32.05370	view",
                     "96.10962	click",
                     "38.61010	click",
                     "96.10962	view",
                     "20.41135	click");
 
    test.runScript();
 
    /*
     * library(infotheo)
     * X=c("98.94791","38.61010","97.10575","62.28313","38.61010","32.05370","96.10962","38.61010","96.10962","20.41135")
     * Y=c("click","click","view","view","view","view","click","click","view","click")
     * condentropy(Y,X)/log(2)
     * [1] 0.4754888 
     */       
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.4754888);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test
  public void dupValStreamingEmpiricalCondEntropoyLog10Test() throws Exception
  {
    PigTest test = createPigTestFromString(condLogEntropy, "type=empirical", "base=log10");
 
    writeLinesToFile("input",
                     "98.94791	click",
                     "38.61010	click",
                     "97.10575	view",
                     "62.28313	view",
                     "38.61010	view",
                     "32.05370	view",
                     "96.10962	click",
                     "38.61010	click",
                     "96.10962	view",
                     "20.41135	click");
    
    test.runScript();
 
    /*
     * library(infotheo)
     * X=c("98.94791","38.61010","97.10575","62.28313","38.61010","32.05370","96.10962","38.61010","96.10962","20.41135")
     * Y=c("click","click","view","view","view","view","click","click","view","click")
     * condentropy(Y,X)/log(10)
     * [1] 0.1431364 
     */      
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.1431364);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5); 
  }

  /**
  register $JAR_PATH

  define CondEntropy datafu.pig.stats.entropy.stream.StreamingCondEntropy();
  
  data = load 'input' as (valX:double, valY:chararray);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     GENERATE CondEntropy(data);
             };
  store data_out into 'output';
   */
  @Multiline private String noOrderCondEntropy;
  
  @Test
  public void noOrderStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(noOrderCondEntropy); 
    
    writeLinesToFile("input",
                     "98.94791	click",
                     "38.61010	view",
                     "97.10575	view",
                     "62.28313	click",
                     "38.83960	click",
                     "32.05370	view",
                     "96.10962	view",
                     "28.72388	click",
                     "96.65888	view",
                     "20.41135	click");

    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "data_out");
         fail( "Testcase should fail");
    } catch(Exception ex) {}
  }

  /**
  register $JAR_PATH

  define CondEntropy datafu.pig.stats.entropy.stream.StreamingCondEntropy();
  
  data = load 'input' as (valX:double);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY *;
                     GENERATE CondEntropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String invalidInputCondEntropy;
 
  @Test
  public void invalidInputStreamingEmpiricalCondEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(invalidInputCondEntropy); 
    
    writeLinesToFile("input",
                     "98.94791",
                     "38.61010",
                     "97.10575",
                     "62.28313",
                     "38.83960",
                     "32.05370",
                     "96.10962",
                     "28.72388",
                     "96.65888",
                     "20.41135");

    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "data_out");
         fail( "Testcase should fail");
    } catch(Exception ex) {
         assertTrue(ex.getMessage().indexOf("The field schema of the input tuple is null or its size is not 2") >= 0);
    }
  }


}
