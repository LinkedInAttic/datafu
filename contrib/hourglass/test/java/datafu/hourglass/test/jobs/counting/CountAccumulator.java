/**
* Copyright 2013 LinkedIn, Inc
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

package datafu.hourglass.test.jobs.counting;

import java.io.Serializable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import datafu.hourglass.model.Accumulator;

public class CountAccumulator implements Accumulator<GenericRecord,GenericRecord>, Serializable
{  
  private long count = 0L;
  private transient GenericRecord output;
  private String schema;
  
  public CountAccumulator(Schema valueSchema)
  {
    schema = valueSchema.toString();
  }
  
  @Override
  public void accumulate(GenericRecord value)
  {
    count += (Long)value.get("count");
  }
  
  private void init()
  {
    if (output == null)
    {
      output = new GenericData.Record(new Schema.Parser().parse(schema));
    }
  }

  @Override
  public GenericRecord getFinal()
  {
    init();
    if (count > 0)
    {
      output.put("count", count);
      return output;
    }
    else
    {
      return null;
    }
  }

  @Override
  public void cleanup()
  {
    this.count = 0L;
  }
}