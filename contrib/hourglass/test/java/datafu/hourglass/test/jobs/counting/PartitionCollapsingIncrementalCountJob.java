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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


import datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.model.Merger;

public abstract class PartitionCollapsingIncrementalCountJob extends AbstractPartitionCollapsingIncrementalJob
{  
  public String getIntermediateValueSchemaName()
  {
    return getClass().getName() + "IntermediateValue";
  }
  
  public String getOutputValueSchemaName()
  {
    return getClass().getName() + "OutputValue";
  }
  
  @Override
  public String getOutputSchemaName()
  {
    return getClass().getName() + "Output";
  }
  
  @Override
  public String getOutputSchemaNamespace()
  {
    return getClass().getPackage().getName();
  }
  
  @Override
  public void config(Configuration conf)
  {
    super.config(conf);    
    conf.set(AbstractCounter.COUNT_VALUE_SCHEMA_PARAM, getIntermediateValueSchema().toString());
  }
  
  public PartitionCollapsingIncrementalCountJob(String name, Properties props) throws IOException
  {
    super(name, props);
  }
  
  @Override
  public Accumulator<GenericRecord,GenericRecord> getReducerAccumulator() 
  {
    return new CountAccumulator(getOutputValueSchema());
  }
  
  @Override
  public Accumulator<GenericRecord,GenericRecord> getCombinerAccumulator() 
  {
    return new CountAccumulator(getIntermediateValueSchema());
  }
  
  @Override
  public Merger<GenericRecord> getRecordMerger()
  {
    return new TheMerger(getOutputValueSchema());
  } 
  
  @Override
  public Merger<GenericRecord> getOldRecordMerger()
  {
    return new OldMerger(getOutputValueSchema());
  }
  
  @Override
  public Mapper<GenericRecord,GenericRecord,GenericRecord> getMapper()
  {
    return getCounter();
  }
  
  protected abstract AbstractCounter getCounter();
  
  public static class TheMerger implements Merger<GenericRecord>
  { 
    private transient Schema schema;
    private String schemaString;
    
    public TheMerger(Schema schema)
    {
      this.schema = schema;
      this.schemaString = schema.toString();
    }
    
    private void init()
    {
      if (schema == null)
      {
        schema = new Schema.Parser().parse(schemaString);
      }
    }
    
    @Override
    public GenericRecord merge(GenericRecord previousOutput, GenericRecord intermediate)
    {
      init();
      long count = 0L;      
      if (previousOutput != null)
      {
        count += (Long)previousOutput.get("count");
      }
      if (intermediate != null)
      {
        count += (Long)intermediate.get("count");
      }
      if (count > 0)
      {
        GenericRecord output = new GenericData.Record(schema);
        output.put("count", count);
        return output;
      }
      else
      {
        return null;
      }
    }
  }
  
  public static class OldMerger implements Merger<GenericRecord>
  {    
    private transient Schema schema;
    private String schemaString;
    
    public OldMerger(Schema schema)
    {
      this.schema = schema;
      this.schemaString = schema.toString();
    }
    
    private void init()
    {
      if (schema == null)
      {
        schema = new Schema.Parser().parse(schemaString);
      }
    }
    
    @Override
    public GenericRecord merge(GenericRecord previousOutput, GenericRecord intermediate)
    { 
      init();
      if (previousOutput == null)
      {
        throw new RuntimeException("Expected previous data");
      }      
      if (intermediate == null)
      {
        throw new RuntimeException("Expected intermediate data");
      }      
      long count = (Long)previousOutput.get("count");      
      count -= (Long)intermediate.get("count");      
      if (count > 0)
      {
        GenericRecord output = new GenericData.Record(schema);
        output.put("count", count);
        return output;
      }
      else
      {
        return null;
      }
    }
  }
      
  @Override
  protected Schema getIntermediateValueSchema()
  {
    return getCountSchema(getIntermediateValueSchemaName());
  }
  
  @Override
  protected Schema getOutputValueSchema()
  {
    return getCountSchema(getOutputValueSchemaName());
  }
  
  protected Schema getCountSchema(String name)
  {
    Schema s = Schema.createRecord(name, null, getOutputSchemaNamespace(), false);
    List<Field> fields = Arrays.asList(new Field("count", Schema.create(Type.LONG), null, null));
    s.setFields(fields);
    return s;
  }
  
  protected static abstract class AbstractCounter implements Mapper<GenericRecord,GenericRecord,GenericRecord>, Configurable
  {       
    private transient Schema _valueSchema;
    private CountWriterImpl writer;
    private Configuration conf;
    
    public static final String COUNT_VALUE_SCHEMA_PARAM = "incremental.count.task.value.schema"; 
    
    @Override
    public Configuration getConf()
    {
      return conf;
    }
    
    @Override
    public void setConf(Configuration conf)
    { 
      this.conf = conf;
      if (conf != null)
      {
        _valueSchema = new Schema.Parser().parse(conf.get(COUNT_VALUE_SCHEMA_PARAM));
        this.writer = new CountWriterImpl();
      }
    }
    
    protected abstract void count(GenericRecord record, CountWriter writer) throws IOException, InterruptedException;    
    
    @Override
    public void map(GenericRecord record, KeyValueCollector<GenericRecord,GenericRecord> context) throws IOException, InterruptedException
    {
      this.writer.setContext(context);
      count(record,this.writer);
    }
    
    private class CountWriterImpl implements CountWriter
    {
      private KeyValueCollector<GenericRecord,GenericRecord> context;
      private GenericRecord value;
      
      public CountWriterImpl()
      {
        value = new GenericData.Record(_valueSchema);        
      }
      
      public void setContext(KeyValueCollector<GenericRecord,GenericRecord> context)
      {
        this.context = context;
      }
      
      @Override
      public void count(GenericRecord key) throws IOException, InterruptedException
      {         
        count(key,1L);
      }

      @Override
      public void count(GenericRecord key, long count) throws IOException, InterruptedException
      {
        value.put("count", count);
        context.collect(key, value);
      }
    }
  }

}
