package datafu.test.pig.bags;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

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
                     "10\t{(1),(2),(3)}",
                     "20\t{(4),(5),(6)}",
                     "30\t{(7),(8)}",
                     "40\t{(9),(10),(11)}",
                     "50\t{(12),(13),(14),(15)}");
    
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
                     "10\t{(1),(2),(3)}",
                     "20\t{(4),(5),(6)}",
                     "30\t{(7),(8)}",
                     "40\t{(9),(10),(11)}",
                     "50\t{(12),(13),(14),(15)}");
    
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
                     "10\t{(1),(2),(3)}",
                     "20\t{(4),(5),(6)}",
                     "30\t{(7),(8)}",
                     "40\t{(9),(10),(11)}",
                     "50\t{(12),(13),(14),(15)}");
    
    test.runScript();
    
    assertOutput(test, "data4",
                 "(10,{(1),(2),(3)},0)",
                 "(20,{(4),(5),(6)},1)",
                 "(30,{(7),(8)},2)",
                 "(40,{(9),(10),(11)},3)",
                 "(50,{(12),(13),(14),(15)},4)");
  }
  
  @Test
  public void comprehensiveBagSplitAndEnumerate() throws Exception
  {
    PigTest test = createPigTest("test/pig/datafu/test/pig/bags/comprehensiveBagSplitAndEnumerate.pig");
    
    writeLinesToFile("input", 
                     "A\t1.0",
                     "B\t2.0",
                     "C\t3.0",
                     "D\t4.0",
                     "E\t5.0");
    
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
                     "A\t1\t0",
                     "B\t2\t0",
                     "C\t3\t0",
                     "D\t4\t0",                     
                     "E\t5\t0");
    
    test.runScript();
    
    assertOutput(test, "data5",
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
                     "Z\t1\t0",
                     "A\t1\t0",
                     "A\t1\t0",
                     "B\t2\t0",
                     "B\t22\t1",
                     "C\t3\t0",
                     "D\t4\t0",                     
                     "E\t5\t0");
    
    test.runScript();
    
    assertOutput(test, "data3", "({(Z,1,0),(A,1,0),(B,2,0),(C,3,0),(D,4,0),(E,5,0)})");
    /*
                 "(A,1)",
                 "(B,2)",
                 "(C,3)",
                 "(D,4)",
                 "(E,5)");
                 */
  }

}
