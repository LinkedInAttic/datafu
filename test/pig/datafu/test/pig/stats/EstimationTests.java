package datafu.test.pig.stats;

import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;
import static org.testng.Assert.*;

public class EstimationTests extends PigTests
{
  /**
  register $JAR_PATH
  
  define HyperLogLogPlusPlus datafu.pig.stats.HyperLogLogPlusPlus();
  
  data_in = LOAD 'input' as (val:int);
    
  data_out = FOREACH (GROUP data_in ALL) GENERATE
    HyperLogLogPlusPlus(data_in) as cardinality;
  
  data_out = FOREACH data_out GENERATE cardinality;
    
  STORE data_out into 'output';
   */
  @Multiline private String hyperLogLogTest;
  
  @Test
  public void hyperLogLogTest() throws Exception
  {
    PigTest test = createPigTestFromString(hyperLogLogTest);

    int count = 1000000;
    String[] input = new String[count];
    for (int i=0; i<count; i++)
    {
      input[i] = Integer.toString(i*10);
    }
    
    writeLinesToFile("input", input);
        
    test.runScript();
    
    List<Tuple> output = getLinesForAlias(test, "data_out", true);
    
    assertEquals(output.size(),1);
    double error = Math.abs(count-((Long)output.get(0).get(0)))/(double)count;
    System.out.println("error: " + error*100.0 + "%");
    assertTrue(error < 0.01);
  }
}
