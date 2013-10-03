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
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;


import datafu.hourglass.jobs.PartitionCollapsingIncrementalJob;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;

/**
 * Given an input event with field "id" stored in daily directories according to yyyy/MM/dd, 
 * incrementally counts the number of events, grouped by each distinct value. 
 * New days will be merged with the previous output.
 * 
 * @author "Matthew Hayes"
 *
 */
public class CountById extends Configured implements NamedTool, Serializable
{
  @Override
  public int run(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      System.err.printf("%s   %s\n",getName(),getDescription());
      System.err.println("Usage: <input> <output>");
      return 1;
    }
    
    try
    {
      run(super.getConf(), args[0], args[1]);
      return 0;
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    return 1;
  }
  
  public void run(Configuration conf, String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
  {
    PartitionCollapsingIncrementalJob job = new PartitionCollapsingIncrementalJob(Examples.class);
    
    job.setConf(conf);
    
    final String namespace = "com.example";
    
    final Schema keySchema = Schema.createRecord("Key",null,namespace,false);
    keySchema.setFields(Arrays.asList(new Field("member_id",Schema.create(Type.LONG),null,null)));
    final String keySchemaString = keySchema.toString(true);
    
    System.out.println("Key schema: " + keySchemaString);
    
    final Schema valueSchema = Schema.createRecord("Value",null,namespace,false);
    valueSchema.setFields(Arrays.asList(new Field("count",Schema.create(Type.INT),null,null)));
    final String valueSchemaString = valueSchema.toString(true);
    
    System.out.println("Value schema: " + valueSchemaString);
    
    job.setKeySchema(keySchema);
    job.setIntermediateValueSchema(valueSchema);
    job.setOutputValueSchema(valueSchema);
    
    job.setInputPaths(Arrays.asList(new Path(inputPath)));
    job.setOutputPath(new Path(outputPath));
    job.setReusePreviousOutput(true);
    job.setRetentionCount(3);
    
    job.setMapper(new Mapper<GenericRecord,GenericRecord,GenericRecord>() 
    {
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
        key.put("member_id", input.get("id"));
        GenericRecord value = new GenericData.Record(vSchema);
        value.put("count", 1);
        collector.collect(key,value);
      }      
    });
    
    job.setReducerAccumulator(new Accumulator<GenericRecord,GenericRecord>() 
    {
      private transient int count;
      private transient Schema vSchema;
      
      @Override
      public void accumulate(GenericRecord value)
      {
        this.count += (Integer)value.get("count");
      }

      @Override
      public GenericRecord getFinal()
      {
        if (vSchema == null) vSchema = new Schema.Parser().parse(valueSchemaString);
        GenericRecord output = new GenericData.Record(vSchema);
        output.put("count", count);
        return output;
      }

      @Override
      public void cleanup()
      {
        this.count = 0;
      }      
    });
    
    job.setCombinerAccumulator(job.getReducerAccumulator());
    job.setUseCombiner(true);
    
    job.run();
  }

  @Override
  public String getName()
  {
    return "countbyid";
  }

  @Override
  public String getDescription()
  {
    return "incrementally counts by id";
  }
}
