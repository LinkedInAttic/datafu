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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.ReduceContext;


import datafu.hourglass.fs.DateRange;
import datafu.hourglass.jobs.DateRangeConfigurable;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.Merger;
import datafu.hourglass.schemas.PartitionCollapsingSchemas;

/**
 * The reducer used by {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Accumulator} is used to perform aggregation and produce the
 * output value.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class CollapsingReducer extends ObjectReducer implements DateRangeConfigurable, Serializable
{  
  protected long _beginTime;
  protected long _endTime;
  private Accumulator<GenericRecord,GenericRecord> _newAccumulator;
  private Accumulator<GenericRecord,GenericRecord> _oldAccumulator;
  private Merger<GenericRecord> _merger;
  private Merger<GenericRecord> _oldMerger;
  private boolean _reusePreviousOutput;
  private PartitionCollapsingSchemas _schemas;
  
  @SuppressWarnings("unchecked")
  public void reduce(Object keyObj,
                     Iterable<Object> values,
                     ReduceContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {
    if (_newAccumulator == null)
    {
      throw new RuntimeException("No reducer set");
    }
    
    GenericRecord key = ((AvroKey<GenericRecord>)keyObj).datum();
    
    // used when processing all data (i.e. no window size)      
    Accumulator<GenericRecord,GenericRecord> acc = getNewAccumulator();
    acc.cleanup();
    long accumulatedCount = 0;
    
    Accumulator<GenericRecord,GenericRecord> accOld = null;
    long oldAccumulatedCount = 0;
    if (getReuseOutput())
    {
      accOld = getOldAccumulator();
      accOld.cleanup();
    }
    
    GenericRecord previous = null;
    
    for (Object valueObj : values)
    {       
      GenericRecord value = ((AvroValue<GenericRecord>)valueObj).datum();
      if (value.getSchema().getFullName().equals(getSchemas().getIntermediateValueSchema().getFullName()))
      {        
        acc.accumulate(value);
        accumulatedCount++;
      }
      else if (value.getSchema().getFullName().equals(getSchemas().getDatedIntermediateValueSchema().getFullName()))
      {          
        if (!_reusePreviousOutput)
        {
          throw new RuntimeException("Did not expect " + getSchemas().getDatedIntermediateValueSchema().getFullName()); 
        }
        
        Long time = (Long)value.get("time");
        GenericRecord data = (GenericData.Record)value.get("value");
        
        if (time == null)
        {
          throw new RuntimeException("time is null");
        }
        
        if (data == null)
        {
          throw new RuntimeException("value is null");
        }
        
        if (time >= _beginTime && time <= _endTime)
        {
          acc.accumulate(data);
          accumulatedCount++;
        }
        else if (time < _beginTime)
        {
          accOld.accumulate(data);
          oldAccumulatedCount++;
        }
        else
        {
          throw new RuntimeException(String.format("Time %d is greater than end time %d",time,_endTime));
        }
      }
      else if (value.getSchema().getFullName().equals(getSchemas().getOutputValueSchema().getFullName()))
      {   
        if (!_reusePreviousOutput)
        {
          throw new RuntimeException("Did not expect " + getSchemas().getDatedIntermediateValueSchema().getFullName()); 
        }
        
        // deep clone the previous output fed back in
        previous = new GenericData.Record((Record)value,true);
      }
      else
      {
        throw new RuntimeException("Unexpected type: " + value.getSchema().getFullName());
      }      
    }
            
    GenericRecord newOutputValue = null;
    GenericRecord oldOutputValue = null;
    
    if (accumulatedCount > 0)
    {
      newOutputValue = acc.getFinal();
    }
    
    if (oldAccumulatedCount > 0)
    {
      oldOutputValue = accOld.getFinal();
    }
    
    GenericRecord outputValue = null;
    
    if (previous == null)
    {
      outputValue = newOutputValue;
      
      if (oldOutputValue != null)
      {
        if (_oldMerger == null)
        {
          throw new RuntimeException("No old record merger set");
        }
        
        outputValue = _oldMerger.merge(outputValue, oldOutputValue);
      }
    }
    else
    {
      outputValue = previous;
      
      if (oldOutputValue != null)
      {
        if (_oldMerger == null)
        {
          throw new RuntimeException("No old record merger set");
        }
        
        outputValue = _oldMerger.merge(outputValue, oldOutputValue);
      }
      
      if (newOutputValue != null)
      {
        if (_merger == null)
        {
          throw new RuntimeException("No new record merger set");
        }
        
        outputValue = _merger.merge(outputValue, newOutputValue);
      }
    }
    
    if (outputValue != null)
    {
      GenericRecord output = new GenericData.Record(getSchemas().getReduceOutputSchema());
      output.put("key", key);
      output.put("value", outputValue);
      context.write(new AvroKey<GenericRecord>(output),null);
    }
  }
  
  /**
   * Sets the Avro schemas.
   * 
   * @param schemas
   */
  public void setSchemas(PartitionCollapsingSchemas schemas)
  {
    _schemas = schemas;
  }
  
  /**
   * Gets the Avro schemas.
   * 
   * @return
   */
  private PartitionCollapsingSchemas getSchemas()
  {
    return _schemas;
  }
  
  /**
   * Gets whether previous output is being reused.
   * 
   * @return true if previous output is reused
   */  
  public boolean getReuseOutput()
  {
    return _reusePreviousOutput;
  }
  
  /**
   * Sets whether previous output is being reused.
   * 
   * @param reuseOutput true if previous output is reused
   */
  public void setReuseOutput(boolean reuseOutput)
  {
    _reusePreviousOutput = reuseOutput;
  }
  
  public void setAccumulator(Accumulator<GenericRecord,GenericRecord> acc)
  {
    _newAccumulator = cloneAccumulator(acc);
    _oldAccumulator = cloneAccumulator(acc);
  }
  
  public Accumulator<GenericRecord,GenericRecord> getNewAccumulator()
  {
    return _newAccumulator;
  }
  
  public Accumulator<GenericRecord,GenericRecord> getOldAccumulator()
  {
    return _oldAccumulator;
  }
  
  public void setRecordMerger(Merger<GenericRecord> merger)
  {
    _merger = merger;
  }
  
  public void setOldRecordMerger(Merger<GenericRecord> merger)
  {
    _oldMerger = merger;
  }
  
  public void setOutputDateRange(DateRange dateRange)
  {
    _beginTime = dateRange.getBeginDate().getTime();
    _endTime = dateRange.getEndDate().getTime();
  }
  
  /**
   * Clone a {@link Accumulator} by serializing and deserializing it.
   * 
   * @param acc The accumulator to clone
   * @return The clone accumulator
   */
  private Accumulator<GenericRecord,GenericRecord> cloneAccumulator(Accumulator<GenericRecord,GenericRecord> acc)
  {
    try
    {
      // clone by serializing
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ObjectOutputStream objStream;
      objStream = new ObjectOutputStream(outputStream);    
      objStream.writeObject(acc);
      objStream.close();
      outputStream.close();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      ObjectInputStream objInputStream = new ObjectInputStream(inputStream);
      @SuppressWarnings("unchecked")
      Accumulator<GenericRecord,GenericRecord> result = (Accumulator<GenericRecord,GenericRecord>)objInputStream.readObject();
      objInputStream.close();
      inputStream.close();
      return result;
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    catch (ClassNotFoundException e)
    {
      throw new RuntimeException(e);
    }
  }
}
