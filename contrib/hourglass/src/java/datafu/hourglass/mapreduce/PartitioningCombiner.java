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

package datafu.hourglass.mapreduce;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.ReduceContext;

import datafu.hourglass.model.Accumulator;

/**
 * The combiner used by {@link datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Accumulator} is used to perform aggregation and produce the
 * intermediate value.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitioningCombiner extends ObjectReducer implements Serializable 
{
  private Accumulator<GenericRecord,GenericRecord> accumulator;
  
  @SuppressWarnings("unchecked")
  public void reduce(Object keyObj,
                      Iterable<Object> values,
                      ReduceContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {           
    Accumulator<GenericRecord,GenericRecord> acc = getAccumulator();
        
    if (acc == null)
    {
      throw new RuntimeException("No accumulator set for combiner!");
    }
    
    acc.cleanup();
                    
    long accumulatedCount = 0;    
    for (Object valueObj : values)
    {       
      AvroValue<GenericRecord> value = (AvroValue<GenericRecord>)valueObj;
      acc.accumulate(value.datum());
      accumulatedCount++;
    }
    
    if (accumulatedCount > 0)
    {
      GenericRecord intermediateValue = acc.getFinal();
      if (intermediateValue != null)
      {
        context.write((AvroKey<GenericRecord>)keyObj,new AvroValue<GenericRecord>(intermediateValue));
      }
    }
  }
  
  /**
   * Gets the accumulator used to perform aggregation. 
   * 
   * @return The accumulator
   */
  public Accumulator<GenericRecord,GenericRecord> getAccumulator()
  {
    return accumulator;
  }
  
  /**
   * Sets the accumulator used to perform aggregation. 
   * 
   * @param acc The accumulator
   */
  public void setAccumulator(Accumulator<GenericRecord,GenericRecord> acc)
  {
    accumulator = acc;
  }  
}
