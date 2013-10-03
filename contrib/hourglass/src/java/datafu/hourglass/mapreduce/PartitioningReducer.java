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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;


import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.schemas.PartitionPreservingSchemas;

/**
 * The reducer used by {@link datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Accumulator} is used to perform aggregation and produce the
 * output value.
 * </p>
 * 
 * <p>
 * The input key is assumed to have time and value fields.  The value here is the true key,
 * and the time represents the input partition the data was derived from.  The true key is
 * used as the key in the reducer output and the time is dropped. 
 * This reducer uses multiple outputs; the time is used to determine which output to write to,
 * where the named outputs have the form yyyyMMdd derived from the time. 
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitioningReducer extends ObjectReducer implements Serializable 
{  
  private transient AvroMultipleOutputs _multipleOutputs;
  private transient Map<Long,String> _timeToNamedOutput;
  private PartitionPreservingSchemas _schemas;
  private Accumulator<GenericRecord,GenericRecord> accumulator;
  
  @SuppressWarnings("unchecked")
  public void reduce(Object keyObj,
                     Iterable<Object> values,
                     ReduceContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {  
    Accumulator<GenericRecord,GenericRecord> acc = getAccumulator();
    
    if (acc == null)
    {
      throw new RuntimeException("No accumulator set for reducer!");
    }
    
    acc.cleanup();
            
    Long keyTime = null;
    
    GenericRecord key = ((AvroKey<GenericRecord>)keyObj).datum();
    
    keyTime = (Long)key.get("time");
    key = (GenericRecord)key.get("value");
    
    long accumulatedCount = 0;    
    for (Object valueObj : values)
    {       
      GenericRecord value = ((AvroValue<GenericRecord>)valueObj).datum(); 
      acc.accumulate(value);      
      accumulatedCount++;      
    }
    
    if (accumulatedCount > 0)
    {
      GenericRecord outputValue = acc.getFinal();               
      if (outputValue != null)
      {                    
        GenericRecord output = new GenericData.Record(getSchemas().getReduceOutputSchema());
        output.put("key", key);
        output.put("value", outputValue);
        
        // write output in directories corresponding to each day
        String namedOutput = getNamedOutput(keyTime);
        if (_multipleOutputs == null)
        {
          throw new RuntimeException("No multiple outputs set");
        }
        _multipleOutputs.write(namedOutput, new AvroKey<GenericRecord>(output), (AvroValue<GenericRecord>)null);
      }
    }
  }
  
  @Override
  public void setContext(TaskInputOutputContext<Object,Object,Object,Object> context)
  {           
    super.setContext(context);
    
    // ... and we also write the final output to multiple directories
    _multipleOutputs = new AvroMultipleOutputs(context);
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
   * Sets the Avro schemas.
   * 
   * @param schemas
   */
  public void setSchemas(PartitionPreservingSchemas schemas)
  {
    _schemas = schemas;
  }
  
  /**
   * Gets the Avro schemas
   * 
   * @return schemas
   */
  public PartitionPreservingSchemas getSchemas()
  {
    return _schemas;
  }

  @Override
  public void close() throws IOException, InterruptedException
  {
    super.close();
    
    if (_multipleOutputs != null)
    {
      _multipleOutputs.close();
      _multipleOutputs = null;
    }
  }
  
  private String getNamedOutput(Long time)
  {
    if (_timeToNamedOutput == null)
    {
      _timeToNamedOutput = new HashMap<Long,String>();
    }
    String namedOutput = _timeToNamedOutput.get(time);
    if (namedOutput == null)
    {
      namedOutput = PathUtils.datedPathFormat.format(new Date(time));
      _timeToNamedOutput.put(time, namedOutput);
    }
    return namedOutput;
  }
}
