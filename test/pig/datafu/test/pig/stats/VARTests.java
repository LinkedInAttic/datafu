package datafu.test.pig.stats;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class VARTests  extends PigTests
{
  /**
  register $JAR_PATH

  define VAR datafu.pig.stats.VAR();
  
  data_in = LOAD 'input' as (val:$VAL_TYPE);
  data_out = GROUP data_in ALL;
  data_out = FOREACH data_out GENERATE VAR(data_in.val) AS variance; 
  
  --describe data_out;
  STORE data_out into 'output';
   */
  @Multiline private String varTest;
  
  @Test
  public void varTestByteArray() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
                                           "VAL_TYPE=bytearray");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestDouble() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"1.0","2.0","3.0","4.0","10.0","5.0","6.0","7.0","8.0","9.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestFloat() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=float");

    String[] input = {"1.0","2.0","3.0","4.0","10.0","5.0","6.0","7.0","8.0","9.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestInteger() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=int");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestLong() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=long");

    String[] input = {"1","2","3","4","10","5","6","7","8","9"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }

  @Test
  public void varTestOneData() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"5.0"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(0.0)");
  }
  
  
  @Test
  public void varTestZeroData() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=long");

    String[] input = {};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),0);
  }
  
  @Test
  public void varTestNullEntry() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=double");

    String[] input = {"1","2","3","4","10","5","6","7","8","9","null"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
  
  @Test
  public void varTestNullEntries() throws Exception
  {
    PigTest test = createPigTestFromString(varTest,
        "VAL_TYPE=float");

    String[] input = {"1","2","3","4","10","5","6","7","8","9","null","null"};
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    assertEquals(output.get(0).toString(), "(8.25)");
  }
 }
