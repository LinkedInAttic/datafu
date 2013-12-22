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
public class EntropyTests extends PigTests
{
  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data = load 'input' as (val:double);
  --describe data;
  data_grouped = GROUP data BY val;
  data_cnt = FOREACH data_grouped GENERATE COUNT(data) AS cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String entropy;
  
  @Test
  public void uniqValEntropoyTest() throws Exception
  {
    System.out.println(entropy);
    PigTest test = createPigTestFromString(entropy); // logarithm base is e 
    
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
     * > entropy(count)
     * [1] 2.302585
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(2.302585);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  @Test
  public void singleValEntropoyTest() throws Exception
  {
    System.out.println(entropy);
    PigTest test = createPigTestFromString(entropy); // logarithm base is e 
    
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
     * > library(entropy)
     * > entropy(count)
     * [1] 0
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  @Test
  public void dupValEntropoyTest() throws Exception
  {
    System.out.println(entropy);
    PigTest test = createPigTestFromString(entropy); // logarithm base is e 
    
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
     * 20.41135  32.0537  38.6101 62.28313 96.10962 97.10575 98.94791 
     * 1        1        3        1        2        1        1 
     * > count=c(1,1,3,1,2,1,1)
     * > library(entropy)
     * > entropy(count)
     * [1] 1.834372
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(1.834372);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  @Test
  public void emptyInputBagEntropoyTest() throws Exception
  {
    System.out.println(entropy);
    PigTest test = createPigTestFromString(entropy); // logarithm base is e 
    
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
     * > entropy(count)
     * [1] 0 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  @Test
  public void singleElemInputBagEntropoyTest() throws Exception
  {
    System.out.println(entropy);
    PigTest test = createPigTestFromString(entropy); // logarithm base is e 
    
    writeLinesToFile("input",
                     "98.94791");

    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > count=c(1)
     * > library(entropy)
     * > entropy(count)
     * [1] 0
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.0);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();

  data = load 'input' as (x:chararray, y:double);
  --describe data;
  data_grouped = GROUP data BY (x, y);
  data_cnt = FOREACH data_grouped GENERATE COUNT(data);
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String pairEntropy;
 
  @Test
  public void dupPairValEntropoyTest() throws Exception
  {
    System.out.println(pairEntropy);
    PigTest test = createPigTestFromString(pairEntropy); // logarithm base is e 
    
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
     * > entropy(count)
     * [1] 1.834372 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(1.834372);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data_cnt = load 'input' as val;
  --describe data_cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String rawByteInputEntropy;
 
  @Test
  public void rawByteInputEntropoyTest() throws Exception
  {
    System.out.println(rawByteInputEntropy);
    PigTest test = createPigTestFromString(rawByteInputEntropy); // logarithm base is 2 
    
    writeLinesToFile("input",
                     "0",
                     "38.0",
                     "0",
                     "62.0",
                     "38",
                     "32.0",
                     "96",
                     "38.0",
                     "96",
                     "0");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > count=c(0, 38.0, 0, 62.0, 38, 32.0, 96, 38.0, 96, 0)
     * > library(entropy)
     * > entropy(count) 
     * [1] 1.846901 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(1.846901);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data_cnt = load 'input' as val:double;
  --describe data_cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String rawDoubleInputEntropy;
 
  @Test
  public void rawDoubleInputEntropoyTest() throws Exception
  {
    System.out.println(rawDoubleInputEntropy);
    PigTest test = createPigTestFromString(rawDoubleInputEntropy); // logarithm base is 2 
    
    writeLinesToFile("input",
                     "0.0",
                     "38.0",
                     "0.0",
                     "62.0",
                     "38.0",
                     "32.001",
                     "96.002",
                     "38.01",
                     "96.00001",
                     "0.0");
        
    test.runScript();
    
    /* Add expected values, computed using R:
     * 
     * e.g.
     * 
     * > count=c(0.0, 38.0, 0.0, 62.0, 38.0, 32.001, 96.002, 38.01, 96.00001, 0.0)
     * > library(entropy)
     * > entropy(count) 
     * [1] 1.846913 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(1.846913);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.4f",entropy),String.format("%.4f", expectationIterator.next()));
    }
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data_cnt = load 'input' as (f1:chararray, f2:chararray);
  --describe data_cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String invalidInputSchemaEntropy;
 
  @Test
  public void invalidInputSchemaEntropoyTest() throws Exception
  {
    System.out.println(invalidInputSchemaEntropy);
    PigTest test = createPigTestFromString(invalidInputSchemaEntropy); // logarithm base is 2 
    
    writeLinesToFile("input",
                     "hadoop	98.94791",
                     "bigdata	38.61010",
                     "hadoop	97.10575",
                     "datafu	32.05370",
                     "bigdata	38.61010",
                     "datafu	32.05370",
                     "datafu	32.05370");
        
    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "data_out");
    } catch(Exception ex) {
         System.out.println(ex);
         return;
    }
 
    fail( "Testcase should fail");    
  }

  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data_cnt = load 'input' as f1:chararray;
  --describe data_cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped GENERATE Entropy(data_cnt);
  store data_out into 'output';
   */
  @Multiline private String invalidInputNumberFormatEntropy;
 
  @Test
  public void invalidInputNumberFormatEntropoyTest() throws Exception
  {
    System.out.println(invalidInputNumberFormatEntropy);
    PigTest test = createPigTestFromString(invalidInputNumberFormatEntropy); // logarithm base is 2 
    
    writeLinesToFile("input",
                     "hadoop",
                     "bigdata");
        
    test.runScript();
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    for (Tuple t : output)
    {
      assertTrue(t.isNull(0));
    }
  }


  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.Entropy();
  
  data = load 'input' as (val:double);
  --describe data;
  data_grouped = GROUP data BY val;
  data_cnt = FOREACH data_grouped GENERATE COUNT(data) AS cnt;
  data_cnt_grouped = GROUP data_cnt ALL;
  data_out = FOREACH data_cnt_grouped  {
                          data_cnt_ordered = order data_cnt by *;
                          GENERATE Entropy(data_cnt_ordered);
                          }
  store data_out into 'output';
   */
  @Multiline private String accumulatedEntropy;

  @Test
  public void accumulatedEntropoyTest() throws Exception
  {
    System.out.println(accumulatedEntropy);
    PigTest test = createPigTestFromString(accumulatedEntropy); // logarithm base is e 
    
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
    
    List<Double> expectedOutput = new ArrayList<Double>();
    //the same output as @test dupValEntropoyTest
    expectedOutput.add(1.834372);
    
    List<Tuple> output = this.getLinesForAlias(test, "data_out");
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    for (Tuple t : output)
    {
      assertTrue(expectationIterator.hasNext());
      Double entropy = (Double)t.get(0);
      assertEquals(String.format("%.5f",entropy),String.format("%.5f", expectationIterator.next()));
    }
  }


}
