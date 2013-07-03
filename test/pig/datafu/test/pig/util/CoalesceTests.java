package datafu.test.pig.util;

import java.util.List;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class CoalesceTests extends PigTests
{
  @Test
  public void coalesceIntTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/coalesceTest.pig");
    
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
  @Test
  public void coalesceLongTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/coalesceLongTest.pig");
    
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
  
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceDiffTypesTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/coalesceDiffTypesTest.pig");
    
    this.writeLinesToFile("input", "1,1,2.0");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data3");
  }
  
  @Test(expectedExceptions=FrontendException.class)
  public void coalesceBagTypeTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/util/coalesceBagTypeTest.pig");
    
    this.writeLinesToFile("input", "1,1,{(2)}");
    
    test.runScript();
    
    this.getLinesForAlias(test, "data3");
  }
}