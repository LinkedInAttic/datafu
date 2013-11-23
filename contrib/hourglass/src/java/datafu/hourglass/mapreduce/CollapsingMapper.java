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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;


import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.schemas.PartitionCollapsingSchemas;

/**
 * The mapper used by {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Mapper} is used for the
 * map operation, which produces key and intermediate value pairs from the input.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class CollapsingMapper extends ObjectMapper implements Serializable
{
  private static Logger _log = Logger.getLogger(CollapsingMapper.class);
  
  private transient IdentityMapCollector _mapCollector;
  private transient TimeMapCollector _timeMapCollector;
 
  private boolean _reusePreviousOutput;
  private PartitionCollapsingSchemas _schemas;  
  private Mapper<GenericRecord,GenericRecord,GenericRecord> _mapper;
    
  @Override
  public void map(Object inputObj, MapContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {
    @SuppressWarnings("unchecked")
    GenericRecord input = ((AvroKey<GenericRecord>)inputObj).datum();   
    try
    {
      getMapCollector().setContext(context);
      getMapper().map(input, getMapCollector());
    }
    catch (InterruptedException e)
    {
      throw new IOException(e);
    }
    catch (UnresolvedUnionException e)
    {
      GenericRecord record = (GenericRecord)e.getUnresolvedDatum();
      _log.error("UnresolvedUnionException on schema: " + record.getSchema());
      throw e;
    } 
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
  
  @Override
  public void setContext(TaskInputOutputContext<Object,Object,Object,Object> context)
  {      
    super.setContext(context);
    
    if (_mapper instanceof Configurable)
    {
      ((Configurable)_mapper).setConf(context.getConfiguration());
    }
  }
  
  /**
   * Gets the mapper.
   * 
   * @return mapper
   */
  public Mapper<GenericRecord,GenericRecord,GenericRecord> getMapper()
  {
    return _mapper;
  }
  
  /**
   * Sets the mapper.
   * 
   * @param mapper
   */
  public void setMapper(Mapper<GenericRecord,GenericRecord,GenericRecord> mapper)
  {
    _mapper = mapper;
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
   * @return schemas
   */
  public PartitionCollapsingSchemas getSchemas()
  {
    return _schemas;
  }
  
  /**
   * Gets the collector used to collect key-value pairs.
   * 
   * @return The collector
   */
  private MapCollector getMapCollector()
  {
    if (getReuseOutput())
    {
      return getTimeMapCollector();
    }
    else
    {
      return getIdentityMapCollector();
    }
  }
  
  /**
   * Gets a collector that maps key-value pairs, where each value
   * is tagged with the partition from which it was derived. 
   * 
   * @return collector
   */
  private TimeMapCollector getTimeMapCollector()
  {
    if (_timeMapCollector == null)
    {
      _timeMapCollector = new TimeMapCollector(getSchemas());
    }
    
    return _timeMapCollector;
  }
  
  /**
   * Gets a collector that maps key-value pairs as-is.
   * 
   * @return collector
   */
  private IdentityMapCollector getIdentityMapCollector()
  {
    if (_mapCollector == null)
    {
      _mapCollector = new IdentityMapCollector(getSchemas());
    }
    
    return _mapCollector;
  }
    
  private abstract class MapCollector implements KeyValueCollector<GenericRecord,GenericRecord>
  {
    private MapContext<Object,Object,Object,Object> context;
    
    public void setContext(MapContext<Object,Object,Object,Object> context)
    {
      this.context = context;
    }
    
    public MapContext<Object,Object,Object,Object> getContext()
    {
      return context;
    }
  }
  
  /**
   * A {@see KeyValueCollector} that outputs key-value pairs to {@link MapContext} 
   * and tags each mapped value with the time for the partition it was derived from.
   * 
   * @author "Matthew Hayes"
   *
   */
  private class TimeMapCollector extends MapCollector
  {
    private GenericRecord wrappedValue;
    private InputSplit lastSplit;
    private long lastTime;
    
    public TimeMapCollector(PartitionCollapsingSchemas schemas)
    {
      this.wrappedValue = new GenericData.Record(schemas.getDatedIntermediateValueSchema());
    }
        
    public void collect(GenericRecord key, GenericRecord value) throws IOException, InterruptedException
    {                
      if (key == null)
      {
        throw new RuntimeException("key is null");
      }
      if (value == null)
      {
        throw new RuntimeException("value is null");
      }
      
      // wrap the value with the time so we know what to merge and what to unmerge        
      long time;        
      if (lastSplit == getContext().getInputSplit())
      {
        time = lastTime;
      }
      else
      {
        FileSplit currentSplit;
        lastSplit = getContext().getInputSplit();
        try
        {
          Method m = getContext().getInputSplit().getClass().getMethod("getInputSplit");
          m.setAccessible(true);
          currentSplit = (FileSplit)m.invoke(getContext().getInputSplit());
        }
        catch (SecurityException e)
        {
          throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e)
        {
          throw new RuntimeException(e);
        }
        catch (IllegalArgumentException e)
        {
          throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
          throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
          throw new RuntimeException(e);
        }
        time = PathUtils.getDateForNestedDatedPath((currentSplit).getPath().getParent()).getTime();
        lastTime = time;
      }
              
      wrappedValue.put("time", time);
      wrappedValue.put("value", value);
      
      getContext().write(new AvroKey<GenericRecord>(key),new AvroValue<GenericRecord>(wrappedValue));
    }
  } 

  /**
   * A {@see KeyValueCollector} that outputs key-value pairs to {@link MapContext} as-is.
   * 
   * @author "Matthew Hayes"
   *
   */
  private class IdentityMapCollector extends MapCollector
  {      
    public IdentityMapCollector(PartitionCollapsingSchemas schemas)
    {
    }
    
    public void collect(GenericRecord key, GenericRecord value) throws IOException, InterruptedException
    {        
      if (key == null)
      {
        throw new RuntimeException("key is null");
      }
      if (value == null)
      {
        throw new RuntimeException("value is null");
      }
      getContext().write(new AvroKey<GenericRecord>(key), new AvroValue<GenericRecord>(value));
    }
  }
}
