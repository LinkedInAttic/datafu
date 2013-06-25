package datafu.test.pig.bags;

import static org.testng.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.pig.bags.CountEach;
import datafu.pig.bags.Enumerate;
import datafu.test.pig.PigTests;


public class BagTests extends PigTests
{
  @Test
  public void nullToEmptyBagTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/nullToEmptyBagTest.pig");
            
    writeLinesToFile("input", 
                     "({(1),(2),(3),(4),(5)})",
                     "()",
                     "{(4),(5)})");
            
    test.runScript();
        
    assertOutput(test, "data2",
                 "({(1),(2),(3),(4),(5)})",
                 "({})",
                 "({(4),(5)})");
  }
  
  @Test
  public void appendToBagTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/appendToBagTest.pig");
    
    writeLinesToFile("input", 
                     "1\t{(1),(2),(3)}\t(4)",
                     "2\t{(10),(20),(30),(40),(50)}\t(60)");
                  
    test.runScript();
            
    assertOutput(test, "data2",
                 "(1,{(1),(2),(3),(4)})",
                 "(2,{(10),(20),(30),(40),(50),(60)})");
  }

   @Test
  public void firstTupleFromBagTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/firstTupleFromBagTest.pig");

    writeLinesToFile("input", "1\t{(4),(9),(16)}");

    test.runScript();

    assertOutput(test, "data2", "(1,(4))");
  }

  
  @Test
  public void prependToBagTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/prependToBagTest.pig");
    
    writeLinesToFile("input", 
                     "1\t{(1),(2),(3)}\t(4)",
                     "2\t{(10),(20),(30),(40),(50)}\t(60)");
                  
    test.runScript();
            
    assertOutput(test, "data2",
                 "(1,{(4),(1),(2),(3)})",
                 "(2,{(60),(10),(20),(30),(40),(50)})");
  }
  
  @Test
  public void bagConcatTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/bagConcatTest.pig");

    writeLinesToFile("input", 
                     "({(1),(2),(3)}\t{(3),(5),(6)}\t{(10),(13)})",
                     "({(2),(3),(4)}\t{(5),(5)}\t{(20)})");
                  
    test.runScript();
            
    assertOutput(test, "data2",
                 "({(1),(2),(3),(3),(5),(6),(10),(13)})",
                 "({(2),(3),(4),(5),(5),(20)})");
  }
  
  @Test
  public void unorderedPairsTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/unorderedPairsTests.pig");
    
    String[] input = {
      "{(1),(2),(3),(4),(5)}"
    };
    
    String[] output = {
        "(1,2)",
        "(1,3)",
        "(1,4)",
        "(1,5)",
        "(2,3)",
        "(2,4)",
        "(2,5)",
        "(3,4)",
        "(3,5)",
        "(4,5)"
      };
    
    test.assertOutput("data",input,"data4",output);
  }
  
  @Test
  public void unorderedPairsTest2() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/unorderedPairsTests2.pig");
        
    this.writeLinesToFile("input", "1\t{(1),(2),(3),(4),(5)}");
    
    test.runScript();
    this.getLinesForAlias(test, "data3");
    
    this.assertOutput(test, "data3",
                      "(1,(1),(2))",
                      "(1,(1),(3))",
                      "(1,(1),(4))",
                      "(1,(1),(5))",
                      "(1,(2),(3))",
                      "(1,(2),(4))",
                      "(1,(2),(5))",
                      "(1,(3),(4))",
                      "(1,(3),(5))",
                      "(1,(4),(5))");    
  }
 
  @Test
  public void bagSplitTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/bagSplitTest.pig",
                                 "MAX=5");
    
    writeLinesToFile("input", 
                     "{(1,11),(2,22),(3,33),(4,44),(5,55),(6,66),(7,77),(8,88),(9,99),(10,1010),(11,1111),(12,1212)}");
    
    test.runScript();
    
    assertOutput(test, "data3",
                 "({(1,11),(2,22),(3,33),(4,44),(5,55)})",
                 "({(6,66),(7,77),(8,88),(9,99),(10,1010)})",
                 "({(11,1111),(12,1212)})");
  }
  
  @Test
  public void bagSplitWithBagNumTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/bagSplitWithBagNumTest.pig",
                                 "MAX=10");
    
    writeLinesToFile("input", 
                     "{(1,11),(2,22),(3,33),(4,44),(5,55),(6,66),(7,77),(8,88),(9,99),(10,1010),(11,1111),(12,1212)}");
    
    test.runScript();
    
    assertOutput(test, "data3",
                 "({(1,11),(2,22),(3,33),(4,44),(5,55),(6,66),(7,77),(8,88),(9,99),(10,1010)},0)",
                 "({(11,1111),(12,1212)},1)");
  }
  
  @Test
  public void enumerateWithReverseTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/enumerateWithReverseTest.pig");
       
    writeLinesToFile("input", 
                     "({(10,{(1),(2),(3)}),(20,{(4),(5),(6)}),(30,{(7),(8)}),(40,{(9),(10),(11)}),(50,{(12),(13),(14),(15)})})");
    
    test.runScript();
    
    assertOutput(test, "data4",
                 "(10,{(1),(2),(3)},5)",
                 "(20,{(4),(5),(6)},4)",
                 "(30,{(7),(8)},3)",
                 "(40,{(9),(10),(11)},2)",
                 "(50,{(12),(13),(14),(15)},1)");
  }
  
  @Test
  public void enumerateWithStartTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/enumerateWithStartTest.pig");
       
    writeLinesToFile("input", 
                     "({(10,{(1),(2),(3)}),(20,{(4),(5),(6)}),(30,{(7),(8)}),(40,{(9),(10),(11)}),(50,{(12),(13),(14),(15)})})");
    
    test.runScript();
    
    assertOutput(test, "data4",
                 "(10,{(1),(2),(3)},1)",
                 "(20,{(4),(5),(6)},2)",
                 "(30,{(7),(8)},3)",
                 "(40,{(9),(10),(11)},4)",
                 "(50,{(12),(13),(14),(15)},5)");
  }
  
  @Test
  public void enumerateTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/enumerateTest.pig");
       
    writeLinesToFile("input",
                     "({(10,{(1),(2),(3)}),(20,{(4),(5),(6)}),(30,{(7),(8)}),(40,{(9),(10),(11)}),(50,{(12),(13),(14),(15)})})");
    
    test.runScript();
    
    assertOutput(test, "data4",
                 "(10,{(1),(2),(3)},0)",
                 "(20,{(4),(5),(6)},1)",
                 "(30,{(7),(8)},2)",
                 "(40,{(9),(10),(11)},3)",
                 "(50,{(12),(13),(14),(15)},4)");
  }
  
  @Test
  public void enumerateTest2() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/enumerateTest.pig");
      
    writeLinesToFile("input",
                     "({(10,{(1),(2),(3)}),(20,{(4),(5),(6)}),(30,{(7),(8)}),(40,{(9),(10),(11)}),(50,{(12),(13),(14),(15)})})",
                     "({(11,{(11),(12),(13),(14)}),(21,{(15),(16),(17),(18)}),(31,{(19),(20)}),(41,{(21),(22),(23),(24)}),(51,{(25),(26),(27)})})");
   
    test.runScript();
   
    assertOutput(test, "data4",
                 "(10,{(1),(2),(3)},0)",
                 "(20,{(4),(5),(6)},1)",
                 "(30,{(7),(8)},2)",
                 "(40,{(9),(10),(11)},3)",
                 "(50,{(12),(13),(14),(15)},4)",
                 "(11,{(11),(12),(13),(14)},0)",
                 "(21,{(15),(16),(17),(18)},1)",
                 "(31,{(19),(20)},2)",
                 "(41,{(21),(22),(23),(24)},3)",
                 "(51,{(25),(26),(27)},4)");
  }  
  
  /* 
   * Testing "Accumulator" part of Enumeration by manually invoking accumulate(), getValue() and cleanup()
   */
  @Test
  public void enumerateAccumulatorTest() throws Exception
  {
    Enumerate enumerate = new Enumerate(); 
    
    Tuple tuple1 = TupleFactory.getInstance().newTuple(1);
    tuple1.set(0, 10);
    
    Tuple tuple2 = TupleFactory.getInstance().newTuple(1);
    tuple2.set(0, 20);
    
    Tuple tuple3 = TupleFactory.getInstance().newTuple(1);
    tuple3.set(0, 30);
    
    Tuple tuple4 = TupleFactory.getInstance().newTuple(1);
    tuple4.set(0, 40);
    
    Tuple tuple5 = TupleFactory.getInstance().newTuple(1);
    tuple5.set(0, 50);
    
    DataBag bag1 = BagFactory.getInstance().newDefaultBag();
    bag1.add(tuple1);
    bag1.add(tuple2);
    bag1.add(tuple3);
    
    DataBag bag2 = BagFactory.getInstance().newDefaultBag();
    bag2.add(tuple4);
    bag2.add(tuple5);
    
    Tuple inputTuple1 = TupleFactory.getInstance().newTuple(1);
    inputTuple1.set(0,bag1);
    
    Tuple inputTuple2 = TupleFactory.getInstance().newTuple(1);
    inputTuple2.set(0,bag2);
    
    enumerate.accumulate(inputTuple1);
    enumerate.accumulate(inputTuple2);
    assertEquals(enumerate.getValue().toString(), "{(10,0),(20,1),(30,2),(40,3),(50,4)}");

    // Testing that cleanup code is correct by calling cleanup() and passing inputs back to Enumerate instance
    enumerate.cleanup();
    enumerate.accumulate(inputTuple1);
    enumerate.accumulate(inputTuple2);
    assertEquals(enumerate.getValue().toString(), "{(10,0),(20,1),(30,2),(40,3),(50,4)}");     
  }
  
  @Test
  public void comprehensiveBagSplitAndEnumerate() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/comprehensiveBagSplitAndEnumerate.pig");
    
    writeLinesToFile("input",
                     "({(A,1.0),(B,2.0),(C,3.0),(D,4.0),(E,5.0)})");
    
    test.runScript();
    
    assertOutput(test, "data_out",
                 // bag #1
                 "(A,1.0,1)",
                 "(B,2.0,1)",
                 "(C,3.0,1)",
                 // bag #2
                 "(D,4.0,2)",
                 "(E,5.0,2)");
  }
  
  @Test
  public void aliasBagFieldsTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/aliasBagFieldsTest.pig");
    
    writeLinesToFile("input",
                     "({(A,1,0),(B,2,0),(C,3,0),(D,4,0),(E,5,0)})");
    
    test.runScript();
    
    assertOutput(test, "data4",
                 "(A,1)",
                 "(B,2)",
                 "(C,3)",
                 "(D,4)",
                 "(E,5)");
  }

  @Test
  public void distinctByTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/distinctByTest.pig");
    
    writeLinesToFile("input",
                     "({(Z,1,0),(A,1,0),(A,1,0),(B,2,0),(B,22,1),(C,3,0),(D,4,0),(E,5,0)})");
    
    test.runScript();
    
    assertOutput(test, "data2",
                 "({(Z,1,0),(A,1,0),(B,2,0),(C,3,0),(D,4,0),(E,5,0)})");
  }
 
  @Test 
  public void countEachTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/countEachTest.pig");

    writeLinesToFile("input", 
                     "({(A),(B),(A),(C),(A),(B)})");
                  
    test.runScript();
            
    assertOutput(test, "data3",
        "({((A),3),((B),2),((C),1)})");
  }
  
  @Test 
  public void countEachExecAndAccumulateTest() throws Exception
  {    
    for (int c=0; c<2; c++)
    {
      CountEach countEach = new CountEach("flatten");
      
      DataBag bag = BagFactory.getInstance().newDefaultBag();
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "A");
        bag.add(t);
      }
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "B");
        bag.add(t);
      }
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "B");
        bag.add(t);
      }
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "C");
        bag.add(t);
      }
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "A");
        bag.add(t);
      }
      { 
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "D");
        bag.add(t);
      }
      
      DataBag output = null;
      
      if (c == 0)
      {
        Tuple input = TupleFactory.getInstance().newTuple(1);
        input.set(0, bag);
        
        System.out.println("Testing exec");
        output = countEach.exec(input);
      }
      else
      {
        System.out.println("Testing accumulate");
        for (Tuple t : bag)
        {
          DataBag tb = BagFactory.getInstance().newDefaultBag();
          tb.add(t);
          Tuple input = TupleFactory.getInstance().newTuple(1);
          input.set(0, tb);
          countEach.accumulate(input);
        }
        
        output = countEach.getValue();
        
        countEach.cleanup();        
        Assert.assertEquals(0, countEach.getValue().size());
      }
      
      System.out.println(output.toString());
      
      Assert.assertEquals(4, output.size());
      Set<String> found = new HashSet<String>();
      for (Tuple t : output)
      {
        String key = (String)t.get(0);    
        found.add(key);  
        if (key == "A")
        {
          Assert.assertEquals(2, t.get(1));
        }
        else if (key == "B")
        {
          Assert.assertEquals(2, t.get(1));
        }
        else if (key == "C")
        {
          Assert.assertEquals(1, t.get(1));
        }
        else if (key == "D")
        {
          Assert.assertEquals(1, t.get(1));
        }
        else
        {
          Assert.fail("Unexpected: " + key);
        }
      }
      Assert.assertEquals(4, found.size());
    }
  }
  
  @Test 
  public void countEachFlattenTest() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/countEachFlattenTest.pig");

    writeLinesToFile("input", 
                     "({(A),(B),(A),(C),(A),(B)})");
                  
    test.runScript();
            
    assertOutput(test, "data3",
        "({(A,3),(B,2),(C,1)})");
  }
}
