package datafu.test.pig.bags.sets;

import java.util.Arrays;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.pig.bags.sets.SetIntersect;
import datafu.test.pig.PigTests;

public class SetTests extends PigTests
{
  @Test
  public void setIntersectTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/sets/setIntersectTest.pig");
    
    String[] input = {
      "{(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)}\t{(0,0),(2,20),(4,40),(8,80)}",
      "{(1,10),(1,10),(2,20),(3,30),(3,30),(4,40),(4,40)}\t{(1,10),(3,30)}"
    };
    
    String[] output = {
        "({(2,20),(4,40)})",
        "({(1,10),(3,30)})"
      };
    
    test.assertOutput("data",input,"data2",output);
  }
  
  @Test
  public void testIntersectWithNullTuples() throws Exception {
     DataBag one = BagFactory.getInstance().newDefaultBag();
     DataBag two = BagFactory.getInstance().newDefaultBag();

     Tuple input = TupleFactory.getInstance().newTuple(Arrays.asList(one, two));
     DataBag output = new SetIntersect().exec(input);
     Assert.assertEquals(0, output.size());
  }

  @Test(expectedExceptions=org.apache.pig.impl.logicalLayer.FrontendException.class)
  public void setIntersectOutOfOrderTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/sets/setIntersectTest.pig");
    
    this.writeLinesToFile("input", 
                          "{(1,10),(3,30),(2,20),(4,40),(5,50),(6,60)}\t{(0,0),(2,20),(4,40),(8,80)}");
        
    test.runScript();
    
    this.getLinesForAlias(test, "data2");
  }
  
  @Test
  public void setUnionTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/sets/setUnionTest.pig");
    
    String[] input = {
        "{(1,10),(1,20),(1,30),(1,40),(1,50),(1,60),(1,80)}\t{(1,1),(1,20),(1,25),(1,25),(1,25),(1,40),(1,70),(1,80)}"
    };
    
    String[] output = {
        "({(1,1),(1,10),(1,20),(1,25),(1,30),(1,40),(1,50),(1,60),(1,70),(1,80)})"
      };
    
    test.assertOutput("data", input, "data2", output);
  }
}
