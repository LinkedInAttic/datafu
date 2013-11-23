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


import datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;

public abstract class PartitionPreservingIncrementalCountJob extends AbstractPartitionPreservingIncrementalJob
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
  
  public PartitionPreservingIncrementalCountJob(String name, Properties props) throws IOException
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
  public Mapper<GenericRecord,GenericRecord,GenericRecord> getMapper()
  {
    return getCounter();
  }
  
  protected abstract AbstractCounter getCounter();
      
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
    
    public AbstractCounter()
    { 
      _valueSchema = null;
    }
    
    @Override
    public Configuration getConf()
    {
      return this.conf;
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
