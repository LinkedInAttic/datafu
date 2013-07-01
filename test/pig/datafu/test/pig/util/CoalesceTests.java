package datafu.test.pig.util;

import java.util.List;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class CoalesceTests extends PigTests
{
  /**
  register $JAR_PATH

  define COALESCE datafu.pig.util.COALESCE();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2:INT,val3:INT);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,val2,val3) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceIntTest;
  
  @Test
  public void coalesceIntTest() throws Exception
  { 
    PigTest test = createPigTestFromString(coalesceIntTest);
    
    this.writeLinesToFile("input", "1,1,2,3",
                                   "2,,2,3",
                                   "3,,,3",
                                   "4,,,",
                                   "5,1,,3",
                                   "6,1,,");
    
    test.runScript();
    
    List<Tuple> lines = this.getLinesForAlias(test, "data3");
    
    Assert.assertEquals(6, lines.size());
    for (Tuple t : lines)
    {
      switch((Integer)t.get(0))
      {
      case 1:
        Assert.assertEquals(1, t.get(1)); break;
      case 2:
        Assert.assertEquals(2, t.get(1)); break;
      case 3:
        Assert.assertEquals(3, t.get(1)); break;
      case 4:
        Assert.assertEquals(null, t.get(1)); break;
      case 5:
        Assert.assertEquals(1, t.get(1)); break;
      case 6:
        Assert.assertEquals(1, t.get(1)); break;
      default:
        Assert.fail("Did not expect: " + t.get(1));                    
      }
    }
  }
  
  /**
  register $JAR_PATH

  define COALESCE datafu.pig.util.COALESCE();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,100L) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  data4 = FOREACH data3 GENERATE testcase, result*100 as result;
  
  STORE data4 INTO 'output';
  */
  @Multiline private static String coalesceLongTest;
  
  @Test
  public void coalesceLongTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceLongTest);
    
    this.writeLinesToFile("input", "1,5",
                                   "2,");
    
    test.runScript();
    
    List<Tuple> lines = this.getLinesForAlias(test, "data4");
    
    Assert.assertEquals(2, lines.size());
    for (Tuple t : lines)
    {
      switch((Integer)t.get(0))
      {
      case 1:
        Assert.assertEquals(500L, t.get(1)); break;
      case 2:
        Assert.assertEquals(10000L, t.get(1)); break;
      default:
        Assert.fail("Did not expect: " + t.get(1));                    
      }
    }
  }
  
  /**
  register $JAR_PATH
  
  define COALESCE datafu.pig.util.COALESCE();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2:DOUBLE);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,val2) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceDiffTypesTest;
  
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceDiffTypesTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceDiffTypesTest);
    
    this.writeLinesToFile("input", "1,1,2.0");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data3");
  }
  
  /**
  register $JAR_PATH
  
  define COALESCE datafu.pig.util.COALESCE();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2: bag {tuple(aVal:int)});
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,val2) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceBagTypeTest;
  
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceBagTypeTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceBagTypeTest);
    
    this.writeLinesToFile("input", "1,1,{(2)}");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data3");
  }
}