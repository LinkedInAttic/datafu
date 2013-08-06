package datafu.test.pig.util;

import java.util.List;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class CoalesceTests extends PigTests
{
  /**
  register $JAR_PATH

  define COALESCE datafu.pig.util.Coalesce();
  
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

  define COALESCE datafu.pig.util.Coalesce();
  
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

  define COALESCE datafu.pig.util.Coalesce();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,100) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  data4 = FOREACH data3 GENERATE testcase, result*100 as result;
  
  STORE data4 INTO 'output';
  */
  @Multiline private static String coalesceCastIntToLongTestFails;
  
  // The first parameter is a long and the fixed value is an int.
  // They cannot be merged without the lazy option.
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceCastIntToLongTestFails() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceCastIntToLongTestFails);
    
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

  define COALESCE datafu.pig.util.Coalesce('lazy');
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,100) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  data4 = FOREACH data3 GENERATE testcase, result*100 as result;
  
  STORE data4 INTO 'output';
  */
  @Multiline private static String coalesceIntAndLongTest;
  
  // The first parameter is a long and the fixed value is an int.
  // They are merged to a long.
  @Test
  public void coalesceCastIntToLongTest1() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceIntAndLongTest);
    
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

  define COALESCE datafu.pig.util.Coalesce('lazy');
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,100L) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  data4 = FOREACH data3 GENERATE testcase, result*100 as result;
  
  STORE data4 INTO 'output';
  */
  @Multiline private static String coalesceIntAndLongTest2;
  
  // The first parameter is an int, but the fixed parameter is a long.
  // They are merged to a long.
  @Test
  public void coalesceCastIntToLongTest2() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceIntAndLongTest2);
    
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

  define COALESCE datafu.pig.util.Coalesce('lazy');
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT);
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,100.0) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  data4 = FOREACH data3 GENERATE testcase, result*100 as result;
  
  STORE data4 INTO 'output';
  */
  @Multiline private static String coalesceIntAndDoubleTest;
  
  // The first parameter is an int, but the fixed parameter is a long.
  // They are merged to a long.
  @Test
  public void coalesceCastIntToDoubleTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceIntAndDoubleTest);
    
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
        Assert.assertEquals(500.0, t.get(1)); break;
      case 2:
        Assert.assertEquals(10000.0, t.get(1)); break;
      default:
        Assert.fail("Did not expect: " + t.get(1));                    
      }
    }
  }
  
  /**
  register $JAR_PATH

  define COALESCE datafu.pig.util.Coalesce();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);
  
  data = FOREACH data GENERATE testcase, (val1 IS NOT NULL ? ToDate(val1) : (datetime)null) as val1;
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,ToDate('1970-01-01T00:00:00.000Z')) as result;
  
  --describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
    
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceCastIntToDatetimeTest;
  
  @Test
  public void coalesceCastIntToDatetimeTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceCastIntToDatetimeTest);
    
    this.writeLinesToFile("input", "1,1375826183000",
                                   "2,");
    
    test.runScript();
    
    List<Tuple> lines = this.getLinesForAlias(test, "data3");
    
    Assert.assertEquals(2, lines.size());
    for (Tuple t : lines)
    {
      Integer testcase = (Integer)t.get(0);
      Assert.assertNotNull(testcase);
      switch(testcase)
      {
      case 1:
        Assert.assertEquals("2013-08-06T21:56:23.000Z", ((DateTime)t.get(1)).toDateTime(DateTimeZone.UTC).toString()); break;
      case 2:
        Assert.assertEquals("1970-01-01T00:00:00.000Z", t.get(1).toString()); break;
      default:
        Assert.fail("Did not expect: " + t.get(1));                    
      }
    }
  }
  
  /**
  register $JAR_PATH

  define COALESCE datafu.pig.util.Coalesce('lazy');
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:LONG);
  
  data = FOREACH data GENERATE testcase, (val1 IS NOT NULL ? ToDate(val1) : (datetime)null) as val1;
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,ToDate('1970-01-01T00:00:00.000Z')) as result;
  
  --describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
    
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceCastIntToDatetimeLazyTest;
  
  @Test
  public void coalesceCastIntToDatetimeLazyTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceCastIntToDatetimeLazyTest);
    
    this.writeLinesToFile("input", "1,1375826183000",
                                   "2,");
    
    test.runScript();
    
    List<Tuple> lines = this.getLinesForAlias(test, "data3");
    
    Assert.assertEquals(2, lines.size());
    for (Tuple t : lines)
    {
      Integer testcase = (Integer)t.get(0);
      Assert.assertNotNull(testcase);
      switch(testcase)
      {
      case 1:
        Assert.assertEquals("2013-08-06T21:56:23.000Z", ((DateTime)t.get(1)).toDateTime(DateTimeZone.UTC).toString()); break;
      case 2:
        Assert.assertEquals("1970-01-01T00:00:00.000Z", t.get(1).toString()); break;
      default:
        Assert.fail("Did not expect: " + t.get(1));                    
      }
    }
  }
  
  /**
  register $JAR_PATH
  
  define COALESCE datafu.pig.util.Coalesce();
  
  data = LOAD 'input' using PigStorage(',') AS (testcase:INT,val1:INT,val2: bag {tuple(aVal:int)});
  
  data2 = FOREACH data GENERATE testcase, COALESCE(val1,val2) as result;
  
  describe data2;
  
  data3 = FOREACH data2 GENERATE testcase, result;
  
  STORE data3 INTO 'output';
  */
  @Multiline private static String coalesceBagIncompatibleTypeTest;
  
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceBagIncompatibleTypeTest() throws Exception
  {
    PigTest test = createPigTestFromString(coalesceBagIncompatibleTypeTest);
    
    this.writeLinesToFile("input", "1,1,{(2)}");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data3");
  }
}