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
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

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
}

