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
 * R's entropy library: http://cran.r-project.org/web/packages/entropy/entropy.pdf
 * used as our test benchmark
 */
public class StreamingChaoShenEntropyTests extends AbstractEntropyTests
{
  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy('$type','$base');
  
  data = load 'input' as (val:double);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY val;
                     GENERATE Entropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String entropy;

  @Test  
  public void uniqValStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(entropy, "type=chaosh", "base=log");
    
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
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c(98.94791,38.61010,97.10575,62.28313,38.83960,32.05370,96.10962,28.72388,96.65888,20.41135) 
     * > table(v)
     * v
     * 20.41135 28.72388  32.0537  38.6101  38.8396 62.28313 96.10962 96.65888 97.10575 98.94791 
     * 1        1        1        1        1        1        1        1        1        1 
     * > count=c(1,1,1,1,1,1,1,1,1,1)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 4.816221
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(4.816221);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test  
  public void singleValStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(entropy, "type=chaosh", "base=log");
    
    writeLinesToFile("input",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791",
                     "98.94791");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c(98.94791,98.94791,98.94791,98.94791,98.94791,98.94791,98.94791,98.94791,98.94791,98.94791) 
     * > table(v)
     * v
     * 98.94791 
     * 10 
     * > count=(10)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 0 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test  
  public void dupValStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(entropy,"type=chaosh", "base=log");
    
    writeLinesToFile("input",
                     "98.94791",
                     "38.61010",
                     "97.10575",
                     "62.28313",
                     "38.61010",
                     "32.05370",
                     "96.10962",
                     "38.61010",
                     "96.10962",
                     "20.41135");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c(98.94791,38.61010,97.10575,62.28313,38.61010,32.05370,96.10962,38.61010,96.10962,20.41135) 
     * > table(v)
     * v
     * 20.41135 28.72388  32.0537  38.6101  38.8396 62.28313 96.10962 96.65888 97.10575 98.94791 
     * 1        1        3        1        2        1        1 
     * > count=c(1,1,3,1,2,1,1)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 2.57429 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(2.57429);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }


  @Test  
  public void emptyInputBagStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(entropy, "type=chaosh", "base=log");
    
    writeLinesToFile("input"
                     );

    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c() 
     * > table(v)
     * < table of extent 0 > 
     * > count=c()
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 0 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test  
  public void singleElemInputBagStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(entropy, "type=chaosh", "base=log");
    
    writeLinesToFile("input",
                     "98.94791");

    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > count=c(1)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 0
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy('$type','$base');

  data = load 'input' as (x:chararray, y:double);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY x,y;
                     GENERATE Entropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String pairEntropy;
 
  @Test  
  public void dupPairValStreamingChaoShenEntropoyTest() throws Exception
  {
    PigTest test = createPigTestFromString(pairEntropy, "type=chaosh", "base=log");
    
    writeLinesToFile("input",
                     "hadoop	98.94791",
                     "bigdata	38.61010",
                     "hadoop	97.10575",
                     "datafu	32.05370",
                     "bigdata	38.61010",
                     "datafu	32.05370",
                     "datafu	32.05370",
                     "hadoop	38.61010",
                     "pig	96.10962",
                     "pig	20.41135");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * > t <- data.table(x=c("hadoop","bigdata","hadoop","datafu","bigdata","datafu","datafu","hadoop","pig","pig"),y=c(98.94791,38.61010,97.10575,32.05370,38.61010,32.05370,32.05370,38.61010,96.10962,20.41135))
     * > t <- t[order(x,y)]
     * > count<-c(2,3,1,1,1,1,1)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log"))
     * [1] 2.57429 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(2.57429);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test  
  public void dupValStreamingChaoShenEntropoyLog2Test() throws Exception
  {
    PigTest test = createPigTestFromString(entropy,"type=chaosh", "base=log2");
    
    writeLinesToFile("input",
                     "98.94791",
                     "38.61010",
                     "97.10575",
                     "62.28313",
                     "38.61010",
                     "32.05370",
                     "96.10962",
                     "38.61010",
                     "96.10962",
                     "20.41135");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c(98.94791,38.61010,97.10575,62.28313,38.61010,32.05370,96.10962,38.61010,96.10962,20.41135) 
     * > table(v)
     * v
     * 20.41135 28.72388  32.0537  38.6101  38.8396 62.28313 96.10962 96.65888 97.10575 98.94791 
     * 1        1        3        1        2        1        1 
     * > count=c(1,1,3,1,2,1,1)
     * > freqs=count/sum(count)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log2"))
     * [1] 3.713915 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(3.713915);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }

  @Test  
  public void dupValStreamingChaoShenEntropoyLog10Test() throws Exception
  {
    PigTest test = createPigTestFromString(entropy, "type=chaosh", "base=log10");
    
    writeLinesToFile("input",
                     "98.94791",
                     "38.61010",
                     "97.10575",
                     "62.28313",
                     "38.61010",
                     "32.05370",
                     "96.10962",
                     "38.61010",
                     "96.10962",
                     "20.41135");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > v=c(98.94791,38.61010,97.10575,62.28313,38.61010,32.05370,96.10962,38.61010,96.10962,20.41135) 
     * > table(v)
     * v
     * 20.41135 28.72388  32.0537  38.6101  38.8396 62.28313 96.10962 96.65888 97.10575 98.94791 
     * 1        1        3        1        2        1        1 
     * > count=c(1,1,3,1,2,1,1)
     * > library(entropy)
     * > entropy(count,count/sum(count),c("CS"),c("log10"))
     * [1] 1.118 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(1.118);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    verifyEqualEntropyOutput(expectedOutput, output, 5);
  }


}
