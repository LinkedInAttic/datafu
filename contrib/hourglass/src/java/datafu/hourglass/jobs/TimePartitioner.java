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

package datafu.hourglass.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A partitioner used by {@link AbstractPartitionPreservingIncrementalJob} to limit the number of named outputs
 * used by each reducer.
 * 
 * <p>
 * The purpose of this partitioner is to prevent a proliferation of small files created by {@link AbstractPartitionPreservingIncrementalJob}.
 * This job writes multiple outputs.  Each output corresponds to a day of input data.  By default records will be distributed across all
 * the reducers.  This means that if many input days are consumed, then each reducer will write many outputs.  These outputs will typically
 * be small.  The problem gets worse as more input data is consumed, as this will cause more reducers to be required. 
 * </p>
 * 
 * <p>
 * This partitioner solves the problem by limiting how many days of input data will be mapped to each reducer.  At the extreme each day of
 * input data could be mapped to only one reducer.  This is controlled through the configuration setting <em>incremental.reducers.per.input</em>,
 * which should be set in the Hadoop configuration.  Input days are assigned to reducers in a round-robin fashion.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class TimePartitioner extends Partitioner<AvroKey<GenericRecord>,AvroValue<GenericRecord>> implements Configurable
{
  public static String INPUT_TIMES = "incremental.input.times";  
  public static String REDUCERS_PER_INPUT = "incremental.reducers.per.input";
  private static String REDUCE_TASKS = "mapred.reduce.tasks";
    
  private int numReducers;
  private Map<Long,List<Integer>> partitionMapping;  
  private Configuration conf;
  
  @Override
  public Configuration getConf()
  {
    return conf;
  }

  @Override
  public void setConf(Configuration conf)
  {
    this.conf = conf;
    
    if (conf.get(REDUCE_TASKS) == null)
    {
      throw new RuntimeException(REDUCE_TASKS + " is required");
    }
    
    this.numReducers = Integer.parseInt(conf.get(REDUCE_TASKS));
    
    if (conf.get(REDUCERS_PER_INPUT) == null)
    {
      throw new RuntimeException(REDUCERS_PER_INPUT + " is required");
    }
    
    int reducersPerInput = Integer.parseInt(conf.get(REDUCERS_PER_INPUT));
    
    this.partitionMapping = new HashMap<Long,List<Integer>>();
    int partition = 0;
    for (String part : conf.get(INPUT_TIMES).split(","))
    {      
      Long day = Long.parseLong(part);
      
      List<Integer> partitions = new ArrayList<Integer>();
      for (int r=0; r<reducersPerInput; r++)
      {
        partitions.add(partition);
        partition = (partition + 1) % this.numReducers;
      }
      
      partitionMapping.put(day,partitions);
    }  
  }
  
  @Override
  public int getPartition(AvroKey<GenericRecord> key, AvroValue<GenericRecord> value, int numReduceTasks)
  {
    if (numReduceTasks != this.numReducers)
    {
      throw new RuntimeException("numReduceTasks " + numReduceTasks + " does not match expected " + this.numReducers);
    }
    
    Long time = (Long)key.datum().get("time");
    if (time == null)
    {
      throw new RuntimeException("time is null");
    }
    
    List<Integer> partitions = this.partitionMapping.get(time);
  
    if (partitions == null)
    {
      throw new RuntimeException("Couldn't find partition for " + time);
    }
    
    GenericRecord extractedKey = (GenericRecord)key.datum().get("value");
    
    if (extractedKey == null)
    {
      throw new RuntimeException("extracted key is null");
    }
    
    int partitionIndex = (extractedKey.hashCode() & Integer.MAX_VALUE) % partitions.size();
    
    return partitions.get(partitionIndex);
  }
}
