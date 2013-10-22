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

package datafu.hourglass.test.jobs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.test.Schemas;

public class ImpressionClickPartitionCollapsingJob extends AbstractPartitionCollapsingIncrementalJob
{
  private static final Schema KEY_SCHEMA;
  private static final Schema INCREMENTAL_VALUE_SCHEMA;
  private static final Schema OUTPUT_VALUE_SCHEMA;
  
  static
  {
    KEY_SCHEMA = Schemas.createRecordSchema(ImpressionClickPartitionCollapsingJob.class, "Key",
                                            new Field("id", Schema.create(Type.LONG), null, null));
    
    INCREMENTAL_VALUE_SCHEMA = Schemas.createRecordSchema(ImpressionClickPartitionCollapsingJob.class, "Value",
                                              new Field("type", Schema.createEnum("Value", null, "datafu.hourglass.test.jobs", Arrays.asList("click","impression")), null, null));
    
    OUTPUT_VALUE_SCHEMA = Schemas.createRecordSchema(ImpressionClickPartitionCollapsingJob.class, "OutputValue",
                                            new Field("clicks", Schema.create(Type.INT), null, null),
                                            new Field("impressions", Schema.create(Type.INT), null, null));
  }
  
  public ImpressionClickPartitionCollapsingJob(String name, Properties props) throws IOException
  {
    super(name, props);
  }

  @Override
  public Accumulator<GenericRecord, GenericRecord> getReducerAccumulator()
  {
    return new Joiner();
  }

  @Override
  public Mapper<GenericRecord, GenericRecord, GenericRecord> getMapper()
  {
    return new TheMapper();
  }
  
  static class Joiner implements Accumulator<GenericRecord,GenericRecord>
  {
    private int clicks;
    private int impressions;
    
    @Override
    public void accumulate(GenericRecord value)
    {
      if (value.get("type").toString().equals("click"))
      {
        clicks++;
      }
      else if (value.get("type").toString().equals("impression"))
      {
        impressions++;
      }
      else
      {
        throw new RuntimeException("Didn't expect: " + value.get("type"));
      }
    }

    @Override
    public GenericRecord getFinal()
    {
      if (clicks > 0 || impressions > 0)
      {
        GenericRecord output = new GenericData.Record(OUTPUT_VALUE_SCHEMA);
        output.put("clicks", clicks);
        output.put("impressions",impressions);
        return output;
      }
      return null;
    }

    @Override
    public void cleanup()
    {
      clicks = 0;
      impressions = 0;
    }
    
  }
  
  static class TheMapper implements Mapper<GenericRecord, GenericRecord, GenericRecord>
  {
    @Override
    public void map(GenericRecord record,
                    KeyValueCollector<GenericRecord, GenericRecord> context) throws IOException,
        InterruptedException
    {
      GenericRecord key = new GenericData.Record(KEY_SCHEMA);
      key.put("id", record.get("id"));
      GenericRecord value = new GenericData.Record(INCREMENTAL_VALUE_SCHEMA);
      if (record.getSchema().getName().contains("Impression"))
      {
        value.put("type","impression");
        context.collect(key, value);
      }
      else if (record.getSchema().getName().contains("Click"))
      {
        value.put("type","click");
        context.collect(key, value);
      }
      else
      {
        throw new RuntimeException("Didn't expect: " + record.getSchema().getName());
      }
    }
  }

  @Override
  protected Schema getKeySchema()
  {
    return KEY_SCHEMA;
  }

  @Override
  protected Schema getIntermediateValueSchema()
  {
    return INCREMENTAL_VALUE_SCHEMA;
  }

  @Override
  protected Schema getOutputValueSchema()
  {
    return OUTPUT_VALUE_SCHEMA;
  }
}
