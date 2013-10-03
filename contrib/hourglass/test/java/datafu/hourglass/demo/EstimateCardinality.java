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

package datafu.hourglass.demo;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import datafu.hourglass.jobs.PartitionCollapsingIncrementalJob;
import datafu.hourglass.jobs.PartitionPreservingIncrementalJob;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;

/**
 * Given an input event with field "id" stored in daily directories according to yyyy/MM/dd, 
 * estimates the cardinality of the values over the past 30 days.
 * 
 * @author "Matthew Hayes"
 *
 */
public class EstimateCardinality extends Configured implements NamedTool, Serializable
{  
  @Override
  public int run(String[] args) throws Exception
  {
    if (args.length != 4)
    {
      System.out.println("Usage: <input> <intermediate> <output> <num-days>");
      System.exit(1);
    }
    
    try
    {
      run(super.getConf(), args[0], args[1], args[2], Integer.parseInt(args[3]));
    }
    catch (IOException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    return 0;
  }
  
  public void run(Configuration conf, String inputPath, String intermediatePath, String outputPath, int numDays) throws IOException, InterruptedException, ClassNotFoundException
  {
    PartitionPreservingIncrementalJob job1 = new PartitionPreservingIncrementalJob(Examples.class);    
    job1.setConf(conf);
    
    final String namespace = "com.example";
    
    final Schema keySchema = Schema.createRecord("Stat",null,namespace,false);
    keySchema.setFields(Arrays.asList(new Field("name", Schema.create(Type.STRING), "the type of statistic", null)));
    final String keySchemaString = keySchema.toString(true);
    
    // data is either an int (the member ID), or bytes from the estimator
    Schema dataSchema = Schema.createUnion(Arrays.asList(Schema.create(Type.LONG),Schema.create(Type.BYTES)));
    
    final Schema valueSchema = Schema.createRecord("Value",null,namespace,false);
    valueSchema.setFields(Arrays.asList(new Field("data", dataSchema, "the data, either a member ID or bytes from the estimator", null),
                                         new Field("count", Schema.create(Type.LONG), "the number of elements", null)));
    final String valueSchemaString = valueSchema.toString(true);
    
    Mapper<GenericRecord,GenericRecord,GenericRecord> mapper = new Mapper<GenericRecord,GenericRecord,GenericRecord>() {
      private transient Schema kSchema;
      private transient Schema vSchema;
      
      @Override
      public void map(GenericRecord input,
                      KeyValueCollector<GenericRecord, GenericRecord> collector) throws IOException,
          InterruptedException
      {
        if (kSchema == null) kSchema = new Schema.Parser().parse(keySchemaString);
        if (vSchema == null) vSchema = new Schema.Parser().parse(valueSchemaString);
        GenericRecord key = new GenericData.Record(kSchema);
        key.put("name", "member_count");
        GenericRecord value = new GenericData.Record(vSchema);
        value.put("data",input.get("id")); // member id
        value.put("count", 1L);             // just a single member
        collector.collect(key,value);        
      }      
    };
    
    Accumulator<GenericRecord,GenericRecord> accumulator = new Accumulator<GenericRecord,GenericRecord>() {
      private transient HyperLogLogPlus estimator;
      private transient Schema vSchema;
      
      @Override
      public void accumulate(GenericRecord value)
      {
        if (estimator == null) estimator = new HyperLogLogPlus(20);
        Object data = value.get("data");
        if (data instanceof Long)
        {
          estimator.offer(data);
        }
        else if (data instanceof ByteBuffer)
        {
          ByteBuffer bytes = (ByteBuffer)data;
          HyperLogLogPlus newEstimator;
          try
          {
            newEstimator = HyperLogLogPlus.Builder.build(bytes.array());
            estimator = (HyperLogLogPlus)estimator.merge(newEstimator);
          }
          catch (IOException e)
          {
            throw new RuntimeException(e);
          }
          catch (CardinalityMergeException e)
          {
            throw new RuntimeException(e);
          }      
        }
      }

      @Override
      public GenericRecord getFinal()
      {
        if (vSchema == null) vSchema = new Schema.Parser().parse(valueSchemaString);
        GenericRecord output = new GenericData.Record(vSchema);
        try
        {
          ByteBuffer bytes = ByteBuffer.wrap(estimator.getBytes());
          output.put("data", bytes);
          output.put("count", estimator.cardinality());
        }
        catch (IOException e)
        {
          throw new RuntimeException(e);
        }
        return output;
      }

      @Override
      public void cleanup()
      {
        estimator = null;
      }      
    };
    
    job1.setKeySchema(keySchema);
    job1.setIntermediateValueSchema(valueSchema);
    job1.setOutputValueSchema(valueSchema);    
    job1.setInputPaths(Arrays.asList(new Path(inputPath)));
    job1.setOutputPath(new Path(intermediatePath));
    job1.setMapper(mapper);
    job1.setCombinerAccumulator(accumulator);
    job1.setReducerAccumulator(accumulator);
    job1.setNumDays(numDays);
    
    job1.run();
    
    PartitionCollapsingIncrementalJob job2 = new PartitionCollapsingIncrementalJob(Examples.class);    
    job2.setConf(conf);
    
    Mapper<GenericRecord,GenericRecord,GenericRecord> identity = new Mapper<GenericRecord,GenericRecord,GenericRecord>()
    {
      @Override
      public void map(GenericRecord record, KeyValueCollector<GenericRecord,GenericRecord> context) throws IOException,
          InterruptedException
      {
        context.collect((GenericRecord)record.get("key"), (GenericRecord)record.get("value"));
      }
    };
    
    job2.setNumDays(30);
    job2.setKeySchema(keySchema);
    job2.setIntermediateValueSchema(valueSchema);
    job2.setOutputValueSchema(valueSchema);    
    job2.setInputPaths(Arrays.asList(new Path(intermediatePath)));
    job2.setOutputPath(new Path(outputPath));
    job2.setMapper(identity);
    job2.setCombinerAccumulator(accumulator);
    job2.setReducerAccumulator(accumulator);
    job2.setNumDays(numDays);
    
    job2.run();
  }

  @Override
  public String getName()
  {
    return "cardinality";
  }

  @Override
  public String getDescription()
  {
    return "estimates cardinality of IDs";
  }
}
