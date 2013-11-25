/*
 * Copyright 2013 LinkedIn Corp. and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package datafu.test.pig.hash;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class HashTests extends PigTests
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
  
  define SHA256 datafu.pig.hash.SHA('256');
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE SHA256(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String sha256Test;
  
  @Test
  public void sha256Test() throws Exception
  {
	  PigTest test = createPigTestFromString(sha256Test);
	  
	  writeLinesToFile("input", 
			  		   "ladsljkasdglk",
                       "lkadsljasgjskdjks",
                       "aladlasdgjks");
	  
	  test.runScript();
	  
	  assertOutput(test, "data_out", 
			  "(70ebaf99c4d8ff8860869e50be2d46afbf150b883f66b50a76ee81cdc802242b)",
			  "(f22e3c744a9ade0fa591d28c55392035248b391c9ee4c77ebfeaf6558c8c0dac)",
			  "(49420b42e764178830783d4520aea56b759f325c1d1167f5640ded91f33f3e69)");
  }
  
  /**
  register $JAR_PATH
  
  define SHA512 datafu.pig.hash.SHA('512');
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE SHA512(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String sha512Test;
  
  @Test
  public void sha512Test() throws Exception
  {
	  PigTest test = createPigTestFromString(sha512Test);
	  
	  writeLinesToFile("input", 
			  		   "ladsljkasdglk",
                       "lkadsljasgjskdjks",
                       "aladlasdgjks");
	  
	  test.runScript();
	  
	  assertOutput(test, "data_out", 
			  "(f681dbd89dfc9edf00f68107ed81b4b7c89abdf84337921785d13d9189937a43decbc264b5013d396a102b18564c39595732c43d6d4cc99473f6d6d7101ecf87)",
			  "(85c130c8636c052e52a2ca091a92d0bb98ee361adcbeeebbd6af978a593b2486a22ac1e7352c683035cfa28de8eee3402adc6760ad54c5c7eda122c5124766bd)",
			  "(3b82af5c08c9ab70523abf56db244eaa6740fa8c356e3a41bb5225560c0949b14b417c8d56e72cc26d5682400e0420a556e692c41ea82855013e8b7bae5fb0fb)");
  }
  
  /**
  register $JAR_PATH

  define MD5 datafu.pig.hash.MD5('base64');
  
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
