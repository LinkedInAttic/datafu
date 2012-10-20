package datafu.test.pig.macros;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class MacrosTests extends PigTests
{ 
  @Test(enabled=false)
  public void rowCountOneRow() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/macros/rowCount.pig");
    
    writeLinesToFile("input",
                     "10"); 
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(1)");
  }
  
  @Test(enabled=false)
  public void rowCountThreeRows() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/macros/rowCount.pig");
    
    writeLinesToFile("input", 
                     "1",
                     "2",
                     "3");
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(3)");
  }
  
  @Test(enabled=false)
  public void rowCountThreeRowsWithNull() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/macros/rowCount.pig");
    
    writeLinesToFile("input", 
                     "1",
                     "",
                     "3",
                     "",
                     "5");
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(5)");
  }
  
  @Test(enabled=false)
  public void countBy() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/macros/countBy.pig");
    
    writeLinesToFile("input", 
                     "a\t1",
                     "b\t2",
                     "c\t3",
                     "a\t",
                     "a\t4",
                     "a\t5",
                     "a\t6",
                     "a\t7",
                     "b\t8",
                     "b\t9");
            
    test.runScript();
    
    Map<String,Number> counts = new HashMap<String,Number>();    
    List<Tuple> tuples = getLinesForAlias(test, "data_out");
    for (Tuple t : tuples)
    {
      String key = (String)t.get(0);
      Number count = (Number)t.get(1);
      counts.put(key,count);
    }
    
    Assert.assertEquals(3, counts.size());
    
    Assert.assertNotNull(counts.get("a"));
    Assert.assertEquals(6,counts.get("a").intValue());
    
    Assert.assertNotNull(counts.get("b"));
    Assert.assertEquals(3,counts.get("b").intValue());
    
    Assert.assertNotNull(counts.get("c"));
    Assert.assertEquals(1,counts.get("c").intValue());
  }
}
