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
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;


import datafu.hourglass.jobs.AbstractNonIncrementalJob;
import datafu.hourglass.test.Schemas;

public class SimpleAvroJob extends AbstractNonIncrementalJob
{
  public static final Schema KEY_SCHEMA;
  public static final Schema VALUE_SCHEMA;
  public static final Schema OUTPUT_SCHEMA;
  
  static
  {
    KEY_SCHEMA = Schemas.createRecordSchema(SimpleAvroJob.class,"Key",
                                       new Field("id", Schema.create(Type.LONG), "ID", null));
    VALUE_SCHEMA = Schemas.createRecordSchema(SimpleAvroJob.class,"Value",
                                               new Field("count", Schema.create(Type.LONG), "count", null));
    OUTPUT_SCHEMA = Schemas.createRecordSchema(SimpleAvroJob.class,"Output",
                                               new Field("id", Schema.create(Type.LONG), "ID", null),
                                               new Field("count", Schema.create(Type.LONG), "count", null));
  }
  
  public SimpleAvroJob(String name, Properties props) throws IOException
  {
    super(name, props);
  }

  @Override
  protected Schema getMapOutputKeySchema()
  {
    return KEY_SCHEMA;
  }

  @Override
  protected Schema getMapOutputValueSchema()
  {
    return VALUE_SCHEMA;
  }

  @Override
  protected Schema getReduceOutputSchema()
  {
    return OUTPUT_SCHEMA;
  }

  @Override
  public Class<? extends BaseMapper> getMapperClass()
  {
    return TheMapper.class;
  }

  @Override
  public Class<? extends BaseReducer> getReducerClass()
  {
    return TheReducer.class;
  }
  
  public static class TheMapper extends BaseMapper
  {
    private final GenericRecord key; 
    private final GenericRecord value;
    
    public TheMapper()
    {
      key = new GenericData.Record(KEY_SCHEMA);
      value = new GenericData.Record(VALUE_SCHEMA);
      value.put("count", 1L);
    }
    
    @Override
    protected void map(AvroKey<GenericRecord> input, NullWritable unused, Context context) throws IOException, InterruptedException
    {
      key.put("id", input.datum().get("id"));
      context.write(new AvroKey<GenericRecord>(key), new AvroValue<GenericRecord>(value));
    }
  }

  public static class TheReducer extends BaseReducer
  {
    private final GenericRecord output;
    
    public TheReducer()
    {
      output = new GenericData.Record(OUTPUT_SCHEMA);
    }
    
    @Override
    protected void reduce(AvroKey<GenericRecord> key,
                          Iterable<AvroValue<GenericRecord>> values,
                          Context context) throws IOException, InterruptedException
    {
      long count = 0L;
      for (AvroValue<GenericRecord> value : values)
      {
        count += (Long)value.datum().get("count");
      }
      output.put("id", key.datum().get("id"));
      output.put("count",count);
      context.write(new AvroKey<GenericRecord>(output), null);
    }
  }
}
