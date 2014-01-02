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

package datafu.test.pig.sessions;

import static org.testng.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.pigunit.PigTest;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import datafu.pig.sessions.SessionCount;
import datafu.pig.sessions.Sessionize;
import datafu.test.pig.PigTests;

public class SessionTests extends PigTests
{
  /**
  register $JAR_PATH

  define Sessionize datafu.pig.sessions.Sessionize('$TIME_WINDOW');
  
  views = LOAD 'input' AS (time:$TIME_TYPE, user_id:int, value:int);
  
  views_grouped = GROUP views BY user_id;
  view_counts = FOREACH views_grouped {
    views = ORDER views BY time;
    GENERATE flatten(Sessionize(views)) as (time,user_id,value,session_id);
  }
  
  max_value = GROUP view_counts BY (user_id, session_id);
  
  max_value = FOREACH max_value GENERATE group.user_id, MAX(view_counts.value) AS val;
  
  STORE max_value INTO 'output';
   */
  @Multiline private String sessionizeTest;
  
  private String[] inputData = new String[] {
      "2010-01-01T01:00:00Z\t1\t10",
      "2010-01-01T01:15:00Z\t1\t20",
      "2010-01-01T01:31:00Z\t1\t10",
      "2010-01-01T01:35:00Z\t1\t20",
      "2010-01-01T02:30:00Z\t1\t30",

      "2010-01-01T01:00:00Z\t2\t10",
      "2010-01-01T01:31:00Z\t2\t20",
      "2010-01-01T02:10:00Z\t2\t30",
      "2010-01-01T02:40:30Z\t2\t40",
      "2010-01-01T03:30:00Z\t2\t50",

      "2010-01-01T01:00:00Z\t3\t10",
      "2010-01-01T01:01:00Z\t3\t20",
      "2010-01-01T01:02:00Z\t3\t5",
      "2010-01-01T01:10:00Z\t3\t25",
      "2010-01-01T01:15:00Z\t3\t50",
      "2010-01-01T01:25:00Z\t3\t30",
      "2010-01-01T01:30:00Z\t3\t15"  
  };
  
  @Test
  public void sessionizeTest() throws Exception
  {
    PigTest test = createPigTestFromString(sessionizeTest,
                                 "TIME_WINDOW=30m",
                                 "JAR_PATH=" + getJarPath(),
                                 "TIME_TYPE=chararray");

    this.writeLinesToFile("input", 
                          inputData);
    
    test.runScript();
    
    HashMap<Integer,HashMap<Integer,Boolean>> userValues = new HashMap<Integer,HashMap<Integer,Boolean>>();
    
    for (Tuple t : this.getLinesForAlias(test, "max_value"))
    {
      Integer userId = (Integer)t.get(0);
      Integer max = (Integer)t.get(1);
      if (!userValues.containsKey(userId))
      {
        userValues.put(userId, new HashMap<Integer,Boolean>());
      }
      userValues.get(userId).put(max, true);
    }
    
    assertEquals(userValues.get(1).size(), 2);
    assertEquals(userValues.get(2).size(), 5);
    assertEquals(userValues.get(3).size(), 1);    
    
    assertTrue(userValues.get(1).containsKey(20));
    assertTrue(userValues.get(1).containsKey(30));
    
    assertTrue(userValues.get(2).containsKey(10));
    assertTrue(userValues.get(2).containsKey(20));
    assertTrue(userValues.get(2).containsKey(30));
    assertTrue(userValues.get(2).containsKey(40));
    assertTrue(userValues.get(2).containsKey(50));    

    assertTrue(userValues.get(3).containsKey(50));
  }
  
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
     
  @Test
  public void sessionizeLongTest() throws Exception
  {
    PigTest test = createPigTestFromString(sessionizeTest,
                                 "TIME_WINDOW=30m",
                                 "JAR_PATH=" + getJarPath(),
                                 "TIME_TYPE=long");

    List<String> lines = new ArrayList<String>();
        
    for (String line : inputData)
    {
      String[] parts = line.split("\t");
      Assert.assertEquals(3, parts.length);
      parts[0] = Long.toString(dateFormat.parse(parts[0]).getTime());
      lines.add(StringUtils.join(parts,"\t"));
    }
    
    this.writeLinesToFile("input", 
                          lines.toArray(new String[]{}));
    
    test.runScript();
    
    HashMap<Integer,HashMap<Integer,Boolean>> userValues = new HashMap<Integer,HashMap<Integer,Boolean>>();
    
    for (Tuple t : this.getLinesForAlias(test, "max_value"))
    {
      Integer userId = (Integer)t.get(0);
      Integer max = (Integer)t.get(1);
      if (!userValues.containsKey(userId))
      {
        userValues.put(userId, new HashMap<Integer,Boolean>());
      }
      userValues.get(userId).put(max, true);
    }
    
    assertEquals(userValues.get(1).size(), 2);
    assertEquals(userValues.get(2).size(), 5);
    
    assertTrue(userValues.get(1).containsKey(20));
    assertTrue(userValues.get(1).containsKey(30));
    
    assertTrue(userValues.get(2).containsKey(10));
    assertTrue(userValues.get(2).containsKey(20));
    assertTrue(userValues.get(2).containsKey(30));
    assertTrue(userValues.get(2).containsKey(40));
    assertTrue(userValues.get(2).containsKey(50));
  }
  
  @Test
  public void sessionizeExecTest() throws Exception
  {
    Sessionize sessionize = new Sessionize("30m");
    Tuple input = TupleFactory.getInstance().newTuple(1);
    DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    input.set(0,inputBag);
    
    Tuple item;
    List<Tuple> result;
    DateTime dt;
    
    // test same session id
    inputBag.clear();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(28).getMillis());
    inputBag.add(item);
    result = toList(sessionize.exec(input));
    
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(2,result.get(0).size());
    Assert.assertEquals(2,result.get(1).size());
    // session ids match
    Assert.assertTrue(result.get(0).get(1).equals(result.get(1).get(1))); 
    
    // test different session id
    inputBag.clear();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(31).getMillis());
    inputBag.add(item);
    result = toList(sessionize.exec(input));
    
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(2,result.get(0).size());
    Assert.assertEquals(2,result.get(1).size());
    // session ids don't match
    Assert.assertFalse(result.get(0).get(1).equals(result.get(1).get(1)));
  }
  
  @Test
  public void sessionizeAccumulateTest() throws Exception
  {
    Sessionize sessionize = new Sessionize("30m");
    Tuple input = TupleFactory.getInstance().newTuple(1);
    DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    input.set(0,inputBag);
    
    Tuple item;
    List<Tuple> result;
    DateTime dt;
    
    // test same session id
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(28).getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    result = toList(sessionize.getValue());
    
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(2,result.get(0).size());
    Assert.assertEquals(2,result.get(1).size());
    // session ids match
    Assert.assertTrue(result.get(0).get(1).equals(result.get(1).get(1))); 
    
    // test different session id
    sessionize.cleanup();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(31).getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    result = toList(sessionize.getValue());
    
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(2,result.get(0).size());
    Assert.assertEquals(2,result.get(1).size());
    // session ids don't match
    Assert.assertFalse(result.get(0).get(1).equals(result.get(1).get(1)));
    
    sessionize.cleanup();
    Assert.assertEquals(0,sessionize.getValue().size());
  }
  
  private List<Tuple> toList(DataBag bag)
  {
    List<Tuple> result = new ArrayList<Tuple>();
    for (Tuple t : bag)
    {
      result.add(t);
    }
    return result;
  }
  
  /**
  register $JAR_PATH

  define SessionCount datafu.pig.sessions.SessionCount('$TIME_WINDOW');
  
  views = LOAD 'input' AS (user_id:int, page_id:int, time:chararray);
  
  views_grouped = GROUP views BY (user_id, page_id);
  view_counts = foreach views_grouped {
    views = order views by time;
    generate group.user_id as user_id, group.page_id as page_id, SessionCount(views.(time)) as count;
  }
  
  STORE view_counts INTO 'output';
   */
  @Multiline
  private String sessionCountPageViewsTest;
  
  @Test
  public void sessionCountPageViewsTest() throws Exception
  {
    PigTest test = createPigTestFromString(sessionCountPageViewsTest,
                                 "TIME_WINDOW=30m",
                                 "JAR_PATH=" + getJarPath());
        
    String[] input = {
      "1\t100\t2010-01-01T01:00:00Z",
      "1\t100\t2010-01-01T01:15:00Z",
      "1\t100\t2010-01-01T01:31:00Z",
      "1\t100\t2010-01-01T01:35:00Z",
      "1\t100\t2010-01-01T02:30:00Z",

      "1\t101\t2010-01-01T01:00:00Z",
      "1\t101\t2010-01-01T01:31:00Z",
      "1\t101\t2010-01-01T02:10:00Z",
      "1\t101\t2010-01-01T02:40:30Z",
      "1\t101\t2010-01-01T03:30:00Z",      

      "1\t102\t2010-01-01T01:00:00Z",
      "1\t102\t2010-01-01T01:01:00Z",
      "1\t102\t2010-01-01T01:02:00Z",
      "1\t102\t2010-01-01T01:10:00Z",
      "1\t102\t2010-01-01T01:15:00Z",
      "1\t102\t2010-01-01T01:25:00Z",
      "1\t102\t2010-01-01T01:30:00Z"
    };
    
    String[] output = {
        "(1,100,2)",
        "(1,101,5)",
        "(1,102,1)"
      };
    
    test.assertOutput("views",input,"view_counts",output);
  }
  
  @Test
  public void sessionCountExecTest() throws Exception
  {
    SessionCount sessionize = new SessionCount("30m");
    Tuple input = TupleFactory.getInstance().newTuple(1);
    DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    input.set(0,inputBag);
    
    Tuple item;
    DateTime dt;
    
    // test same session id
    inputBag.clear();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(28).getMillis());
    inputBag.add(item);
    Assert.assertEquals(1L,sessionize.exec(input).longValue()); 
    
    // test different session id
    inputBag.clear();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(31).getMillis());
    inputBag.add(item);
    Assert.assertEquals(2L,sessionize.exec(input).longValue());
  }
  
  @Test
  public void sessionCountAccumulateTest() throws Exception
  {
    SessionCount sessionize = new SessionCount("30m");
    Tuple input = TupleFactory.getInstance().newTuple(1);
    DataBag inputBag = BagFactory.getInstance().newDefaultBag();
    input.set(0,inputBag);
    
    Tuple item;
    DateTime dt;
    
    // test same session id
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(28).getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    Assert.assertEquals(1L,sessionize.getValue().longValue()); 
    
    // test different session id
    sessionize.cleanup();
    dt = new DateTime();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    item = TupleFactory.getInstance().newTuple(1);
    item.set(0, dt.plusMinutes(31).getMillis());
    inputBag.add(item);
    sessionize.accumulate(input);
    inputBag.clear();
    Assert.assertEquals(2L,sessionize.exec(input).longValue());
    
    sessionize.cleanup();
    Assert.assertEquals(0,sessionize.getValue().longValue());
  }
}

