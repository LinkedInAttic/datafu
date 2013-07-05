package datafu.test.pig.hash;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class HashTests  extends PigTests
{
  /**
  register $JAR_PATH

  define MD5 datafu.pig.hash.MD5();
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE MD5(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String md5Test;
  
  @Test
  public void md5Test() throws Exception
  {
    PigTest test = createPigTestFromString(md5Test);
    
    writeLinesToFile("input", 
                     "ladsljkasdglk",
                     "lkadsljasgjskdjks",
                     "aladlasdgjks");
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(d9a82575758bb4978949dc0659205cc6)",
                 "(9ec37f02fae0d8d6a7f4453a62272f1f)",
                 "(cb94139a8b9f3243e68a898ec6bd9b3d)");
  }
  
  /**
  register $JAR_PATH

  define MD5 datafu.pig.hash.MD5Base64();
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE MD5(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String md5Base64Test;
  
  @Test
  public void md5Base64Test() throws Exception
  {
    PigTest test = createPigTestFromString(md5Base64Test);
    
    writeLinesToFile("input", 
                     "ladsljkasdglk",
                     "lkadsljasgjskdjks",
                     "aladlasdgjks");
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(2agldXWLtJeJSdwGWSBcxg==)",
                 "(nsN/Avrg2Nan9EU6YicvHw==)",
                 "(y5QTmoufMkPmiomOxr2bPQ==)");
  }
}
