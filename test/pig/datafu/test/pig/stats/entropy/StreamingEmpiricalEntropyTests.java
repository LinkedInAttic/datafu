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
public class StreamingEmpiricalEntropyTests extends PigTests
{
  /**
  register $JAR_PATH

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy();
  
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
  public void uniqValStreamingEmpiricalEntropoyTest() throws Exception
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
  public void singleValStreamingEmpiricalEntropoyTest() throws Exception
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
  public void dupValStreamingEmpiricalEntropoyTest() throws Exception
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
  public void emptyInputBagStreamingEmpiricalEntropoyTest() throws Exception
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
  public void singleElemInputBagStreamingEmpiricalEntropoyTest() throws Exception
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

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy();
  
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
  public void dupPairValStreamingEmpiricalEntropoyTest() throws Exception
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

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy('$type', '$base');
  
  data = load 'input' as (val:double);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     data_ordered = ORDER data BY val;
                     GENERATE Entropy(data_ordered);
             };
  store data_out into 'output';
   */
  @Multiline private String logEntropy;
 
  @Test
  public void dupValStreamingEmpiricalEntropoyLog2Test() throws Exception
  {
    System.out.println(logEntropy);
    PigTest test = createPigTestFromString(logEntropy, "type=empirical", "base=2"); // logarithm base is 2 
    
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
     * > entropy(count, freqs, c("ML"), c("log2")) 
     * [1] 2.646439 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(2.646439);
    
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
  public void dupValStreamingEmpiricalEntropoyLog10Test() throws Exception
  {
    System.out.println(logEntropy);
    PigTest test = createPigTestFromString(logEntropy, "type=empirical", "base=10"); // logarithm base is 10 
    
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
     * > entropy(count, freqs, c("ML"), c("log10")) 
     * [1] 0.7966576 
     * 
     */
    List<Double> expectedOutput = new ArrayList<Double>();
    expectedOutput.add(0.7966576);
    
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

  define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy();
  
  data = load 'input' as (val:double);
  --describe data;
  data_grouped = GROUP data ALL;
  data_out = FOREACH data_grouped {
                     GENERATE Entropy(data);
             };
  store data_out into 'output';
   */
  @Multiline private String noOrderEntropy;
 
  @Test
  public void noOrderStreamingEmpiricalEntropoyTest() throws Exception
  {
    System.out.println(noOrderEntropy);
    PigTest test = createPigTestFromString(noOrderEntropy); // logarithm base is e 
    
    writeLinesToFile("input",
                     "98.94791",
                     "38.61010",
                     "38.61010",
                     "37.10575",
                     "62.28313",
                     "38.61010",
                     "32.05370",
                     "96.10962",
                     "38.61010",
                     "96.10962",
                     "20.41135");

    try {
         test.runScript();
         List<Tuple> output = this.getLinesForAlias(test, "data_out");
    } catch(Exception ex) {
         System.out.println(ex);
         return;
    }
 
    fail( "Testcase should fail");
  }
}
