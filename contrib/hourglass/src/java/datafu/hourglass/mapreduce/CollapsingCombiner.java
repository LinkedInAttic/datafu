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
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.ReduceContext;


import datafu.hourglass.fs.DateRange;
import datafu.hourglass.jobs.DateRangeConfigurable;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.schemas.PartitionCollapsingSchemas;

/**
 * The combiner used by {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Accumulator} is used to perform aggregation and produce the
 * intermediate value.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class CollapsingCombiner extends ObjectReducer implements DateRangeConfigurable, Serializable
{  
  private Accumulator<GenericRecord,GenericRecord> _accumulator;
  private boolean _reusePreviousOutput;
  private PartitionCollapsingSchemas _schemas;
  private long _beginTime;
  private long _endTime;
  
  @SuppressWarnings("unchecked")
  public void reduce(Object keyObj,
                      Iterable<Object> values,
                      ReduceContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {
    Accumulator<GenericRecord,GenericRecord> acc = getAccumulator();
    
    if (acc == null)
    {
      throw new RuntimeException("No combiner factory set");
    }
    
    long accumulatedCount = 0;
    
    acc.cleanup();
    
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
          // pass through unchanged, reducer will handle it
          context.write((AvroKey<GenericRecord>)keyObj,new AvroValue<GenericRecord>(value));
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
          throw new RuntimeException("Did not expect " + getSchemas().getOutputValueSchema().getFullName()); 
        }
                
        // pass through unchanged, reducer will handle it
        context.write((AvroKey<GenericRecord>)keyObj,new AvroValue<GenericRecord>(value));
      }
      else
      {
        throw new RuntimeException("Unexpected type: " + value.getSchema().getFullName());
      }      
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
   * Sets the schemas.
   * 
   * @param schemas
   */
  public void setSchemas(PartitionCollapsingSchemas schemas)
  {
    _schemas = schemas;
  }
  
  /**
   * Gets the schemas.
   * 
   * @return schemas
   */
  public PartitionCollapsingSchemas getSchemas()
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
  
  /**
   * Gets the accumulator used to perform aggregation. 
   * 
   * @return The accumulator
   */
  public Accumulator<GenericRecord,GenericRecord> getAccumulator()
  {
    return _accumulator;
  }
  
  /**
   * Sets the accumulator used to perform aggregation. 
   * 
   * @param acc The accumulator
   */
  public void setAccumulator(Accumulator<GenericRecord,GenericRecord> acc)
  {
    _accumulator = cloneAccumulator(acc);
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
