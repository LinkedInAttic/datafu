/*
 * Copyright 2010 LinkedIn, Inc
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
 
package datafu.pig.sessions;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.Period;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Performs a count of events, ignoring events which occur within the
 * same time window.
 * <p>
 * This is useful for tasks such as counting the number of page views per user since it:
 *  a) prevent reloads and go-backs from overcounting actual views
 *  b) captures the notion that views across multiple sessions are more meaningful
 * <p>
 * Input <b>must</b> be sorted ascendingly by time for this UDF to work.
 * <p>
 * Example:
 * <pre>
 * {@code
 * 
 * %declare TIME_WINDOW  10m
 * 
 * define SessionCount datafu.pig.sessions.SessionCount('$TIME_WINDOW');
 * 
 * views = LOAD 'views' as (user_id:int, page_id:int, time:chararray);
 * views_grouped = GROUP views by (user_id, page_id);
 * view_counts = FOREACH views_grouped { 
 *   views = order views by time;
 *   generate group.user_id as user_id, 
 *            group.page_id as page_id, 
 *            SessionCount(views.(time)) as count; }
 * }
 * </pre>
 * 
 */
public class SessionCount extends SimpleEvalFunc<Long>
{
  private final long millis;

  public SessionCount(String timeSpec)
  {
    Period p = new Period("PT" + timeSpec.toUpperCase());
    this.millis = p.toStandardSeconds().getSeconds() * 1000;
  }

  public Long call(DataBag bag) throws IOException
  {
    DateTime last_date = null;
    long sum = 0;
    
    for (Tuple t : bag) {
      DateTime date = new DateTime(t.get(0));

      if (last_date == null) {
        last_date = date;
        sum = 1;
      } else if (date.isAfter(last_date.plus(this.millis)))
        sum += 1;
      else if (date.isBefore(last_date))
        throw new IOException("input time series is not sorted");

      last_date = date;
    }

    return sum;
  }
}
