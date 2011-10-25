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
import java.util.UUID;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * Sessionizes an input stream.
 *
 * This UDF takes a constructor argument which is the session timeout (an idle
 * period of this amount indicates that a new session has started) and assumes
 * the first element of the input bag is an ISO8601 timestamp. The input bag
 * must be sorted by this timestamp. It returns the input bag with a new field,
 * session_id, that is a GUID indicating the session of the request.
 *
 * Example:
 * <pre>
 * {@code
 * 
 * %declare TIME_WINDOW  30m
 * 
 * define Sessionize datafu.pig.sessions.Sessionize('$TIME_WINDOW');
 *
 * -- sessionize the visit stream
 * VIEWS = group VIEWS by member_id;
 * SESSIONS = foreach VIEWS {
 *   VISITS = order VIEWS by visit_date;
 *   generate FLATTEN(Sessionize(VISITS)) as (visit_date,member_id,url,session_id); 
 * }
 *
 * -- count the number of sessions hitting the url
 * ROLLUP = group SESSIONS by url;
 * RESULT = foreach ROLLUP generate group as url, COUNT(SESSIONS) as session_cnt;
 * }
 * </pre>
 */
public class Sessionize extends EvalFunc<DataBag> implements Accumulator<DataBag>
{
  private final long millis;

  private DataBag outputBag;
  private DateTime last_date;
  private String id;

  public Sessionize(String timeSpec)
  {
    Period p = new Period("PT" + timeSpec.toUpperCase());
    this.millis = p.toStandardSeconds().getSeconds() * 1000;

    cleanup();
  }

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    accumulate(input);
    DataBag outputBag = getValue();
    cleanup();

    return outputBag;
  }

  @Override
  public void accumulate(Tuple input) throws IOException
  {
    for (Tuple t : (DataBag) input.get(0)) {
      String timeString = (String)t.get(0);
      DateTime date = new DateTime(timeString);

      if (this.last_date == null)
        this.last_date = date;
      else if (date.isAfter(this.last_date.plus(this.millis)))
        this.id = UUID.randomUUID().toString();
      else if (date.isBefore(last_date))
        throw new IOException(String.format("input time series is not sorted (%s < %s)", date, last_date));

      Tuple t_new = TupleFactory.getInstance().newTuple(t.getAll());
      t_new.append(this.id);
      outputBag.add(t_new);
      
      this.last_date = date;
    }
  }

  @Override
  public DataBag getValue()
  {
    return outputBag;
  }

  @Override
  public void cleanup()
  {
    this.last_date = null;
    this.outputBag = BagFactory.getInstance().newDefaultBag();
    this.id = UUID.randomUUID().toString();
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      Schema inputBagSchema = inputFieldSchema.schema;
      
      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      if (inputTupleSchema.getField(0).type != DataType.CHARARRAY)
      {
        throw new RuntimeException(String.format("Expected first element of tuple to be a CHARARRAY, but instead found %s",
                                                 DataType.findTypeName(inputTupleSchema.getField(0).type)));
      }
      
      Schema outputTupleSchema = inputTupleSchema.clone();
      outputTupleSchema.add(new Schema.FieldSchema("session_id", DataType.CHARARRAY));      
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                             .getName()
                                                             .toLowerCase(), input),
                                           outputTupleSchema,
                                           DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
