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

package datafu.test.pig.urls;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class UserAgentTest extends PigTests
{
  
  /**
  register $JAR_PATH

  define UserAgentClassify datafu.pig.urls.UserAgentClassify();
  
  data = load 'input' as (usr_agent:chararray);
  data_out = foreach data generate UserAgentClassify(usr_agent) as class;
  --describe data_out;
  store data_out into 'output';
   */
  @Multiline private String userAgentTest;
  
  @Test
  public void userAgentTest() throws Exception
  {
    PigTest test = createPigTestFromString(userAgentTest);
  
    String[] input = {
        "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
        "Mozilla/5.0 (compatible; Konqueror/3.5; Linux; X11; de) KHTML/3.5.2 (like Gecko) Kubuntu 6.06 Dapper",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.2a1pre) Gecko/20110331 Firefox/4.2a1pre Fennec/4.1a1pre",
        "Opera/9.00 (X11; Linux i686; U; en)",
        "Wget/1.10.2",
        "Opera/9.80 (Android; Linux; Opera Mobi/ADR-1012221546; U; pl) Presto/2.7.60 Version/10.5",
        "Mozilla/5.0 (Linux; U; Android 2.2; en-us; DROID2 Build/VZW) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1"
    };
    
    String[] output = {
        "(mobile)",
        "(desktop)",
        "(mobile)",
        "(desktop)",
        "(desktop)",
        "(mobile)",
        "(mobile)",
      };
    
    test.assertOutput("data",input,"data_out",output);
  }

}
