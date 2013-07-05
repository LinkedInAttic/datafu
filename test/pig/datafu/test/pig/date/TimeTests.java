package datafu.test.pig.date;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class TimeTests extends PigTests
{  
  /**
  register $JAR_PATH

  define TimeCount datafu.pig.date.TimeCount('$TIME_WINDOW');
  
  views = LOAD 'input' AS (user_id:int, page_id:int, time:chararray);
  
  views_grouped = GROUP views BY (user_id, page_id);
  view_counts = foreach views_grouped {
    views = order views by time;
    generate group.user_id as user_id, group.page_id as page_id, TimeCount(views.(time)) as count;
  }
  
  STORE view_counts INTO 'output';
   */
  @Multiline
  private String timeCountPageViewsTest;
  
  @Test
  public void timeCountPageViewsTest() throws Exception
  {
    PigTest test = createPigTestFromString(timeCountPageViewsTest,
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
