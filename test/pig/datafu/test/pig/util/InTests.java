package datafu.test.pig.util;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class InTests extends PigTests
{
  /**
  register $JAR_PATH

  define In datafu.pig.util.In();
  
  data = LOAD 'input' AS (B: bag {T: tuple(v:INT)});
  
  data2 = FOREACH data {
    C = FILTER B By In(v, 1,2,3);
    GENERATE C;
  }
  
  describe data2;
  
  STORE data2 INTO 'output';
  */
  @Multiline private static String inIntTest;
  
  @Test
  public void inIntTest() throws Exception
  { 
    PigTest test = createPigTestFromString(inIntTest);
    
    writeLinesToFile("input", 
                     "({(1),(2),(3),(4),(5)})",
                     "({(1),(2)})",
                     "({(4),(5)})");
    
    test.runScript();
    
    assertOutput(test, "data2",
                 "({(1),(2),(3)})",
                 "({(1),(2)})",
                 "({})");    
  }
  
  /**
  register $JAR_PATH

  define In datafu.pig.util.In();
  
  data = LOAD 'input' AS (B: bag {T: tuple(v:chararray)});
  
  data2 = FOREACH data {
    C = FILTER B By In(v, 'will','matt','sam');
    GENERATE C;
  }
  
  describe data2;
  
  STORE data2 INTO 'output';
  */
  @Multiline private static String inStringTest;
  
  @Test
  public void inStringTest() throws Exception
  { 
    PigTest test = createPigTestFromString(inStringTest);
    
    writeLinesToFile("input", 
                     "({(alice),(bob),(will),(matt),(sam)})",
                     "({(will),(matt)})",
                     "({(alice),(bob)})");
    
    test.runScript();
    
    assertOutput(test, "data2",
                 "({(will),(matt),(sam)})",
                 "({(will),(matt)})",
                 "({})");    
  }
  
  /**
  register $JAR_PATH
  
  define In datafu.pig.util.In();
  
  data = LOAD 'input' AS (owner:chararray, color:chararray);
  describe data;
  data2 = FILTER data BY In(color, 'red','blue');
  describe data2;
  STORE data2 INTO 'output';
  */
  @Multiline private static String inTopLevelTest;
  
  @Test
  public void inTopLevelTest() throws Exception
  {
    PigTest test = createPigTestFromString(inTopLevelTest);
    
    writeLinesToFile("input", 
                     "alice\tred",
                     "bob\tblue",
                     "charlie\tgreen",
                     "dave\tred");
    test.runScript();
    
    assertOutput(test, "data2", 
                 "(alice,red)",
                 "(bob,blue)",
                 "(dave,red)");
  }
  
}