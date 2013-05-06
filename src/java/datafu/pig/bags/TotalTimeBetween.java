package datafu.pig.bags;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * This UDF takes an input bag which is assumed to be ordered by
 * an ISO8601 timestamp field and returns the total time elapsed
 * (in seconds) by all tuples in the bag.  The first field in 
 * each of the tuples in the DataBag is assumed to be the ISO8601
 * timestamp.
 * 
 * Example:
 * {@code
 * DEFINE TotalTimeBetween datafu.pig.bags.TotalTimeBetween();
 * 
 * -- input: 
 * -- 2012-01-01T00:00:00\tbiuA8n98wn\thttp://www.google.com/
 * -- 2012-01-01T00:00:00\tasd909m09j\thttp://www.google.com/
 * -- 2012-01-01T00:01:00\tbiuA8n98wn\thttp://www.google.com/1
 * -- 2012-01-01T00:01:05\tbiuA8n98wn\thttp://www.google.com/
 * -- 2012-01-01T00:02:00\tasd909m09j\thttp://www.google.com/2
 * input = LOAD 'input' AS (timestamp:chararray, visitor_id:chararray, url:chararray);
 * by_visitor = GROUP data BY visitor_id;
 * count_time = FOREACH by_visitor &#123;
 *   ordered = ORDER data BY timestamp ASC;
 *   GENERATE group, TotalTimeBetween(ordered);
 * &#125;;
 * DUMP count_time;
 * -- (biuA8n98wn,65)
 * -- (asd909m09j,120)
 * }
 * 
 *
 */
public class TotalTimeBetween extends EvalFunc<Long> implements Accumulator<Long> {
  
  private DateTime intermediateLastTime;
  private long intermediateTimeInSeconds;
  private DateTimeFormatter isoParser;
  
  public TotalTimeBetween() {
    isoParser = ISODateTimeFormat.dateTimeParser();
    cleanup();
  }
  
  public void cleanup() {
    intermediateTimeInSeconds = 0L;
    intermediateLastTime = null;
  }

  public Long exec(Tuple inputTuple) throws IOException {
    accumulate(inputTuple);
    long timeInSeconds = getValue();
    cleanup();
    
    return timeInSeconds;
  }
  
  public void accumulate(Tuple inputTuple) throws ExecException {
    DataBag inputBag = (DataBag)inputTuple.get(0);
    for (Tuple t : inputBag) {
      DateTime tupleTime = isoParser.parseDateTime((String) t.get(0));
      if (intermediateLastTime == null) {
        intermediateLastTime = tupleTime;
        continue;
      }
      
      intermediateTimeInSeconds += Seconds.secondsBetween(intermediateLastTime, tupleTime).getSeconds();
      intermediateLastTime = tupleTime;
    }
  }
  
  public Long getValue() {
    return intermediateTimeInSeconds;
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    return new Schema(new Schema.FieldSchema(null, DataType.LONG));
  }
}
