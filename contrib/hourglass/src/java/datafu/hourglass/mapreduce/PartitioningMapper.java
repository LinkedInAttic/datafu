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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.model.KeyValueCollector;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.schemas.PartitionPreservingSchemas;

/**
 * The mapper used by {@link datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob} and its derived classes.
 * 
 * <p>
 * An implementation of {@link datafu.hourglass.model.Mapper} is used for the
 * map operation, which produces key and intermediate value pairs from the input.
 * The input to the mapper is assumed to be partitioned by day.
 * Each key produced by {@link datafu.hourglass.model.Mapper} is tagged with the time for the partition
 * that the input came from.  This enables the combiner and reducer to preserve the partitions.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitioningMapper extends ObjectMapper implements Serializable
{
  private transient MapCollector _mapCollector;
  private transient FileSplit _lastSplit;
  private transient long _lastTime;
  private Mapper<GenericRecord,GenericRecord,GenericRecord> _mapper;
  private PartitionPreservingSchemas _schemas;
  
  @SuppressWarnings("unchecked")
  @Override
  public void map(Object inputObj, MapContext<Object,Object,Object,Object> context) throws IOException, InterruptedException
  {  
    long time;
    
    if (_lastSplit == context.getInputSplit())
    {
      time = _lastTime;
    }
    else
    {
      _lastSplit = (FileSplit)context.getInputSplit();      
      time = PathUtils.getDateForNestedDatedPath((_lastSplit).getPath().getParent()).getTime();
      _lastTime = time;
    }
    
    getMapCollector().setContext(context);
    
    // Set the time, representing the time range this data was derived from.
    // The key is tagged with this time.
    getMapCollector().setTime(time);
    
    try
    {
      AvroKey<GenericRecord> input = (AvroKey<GenericRecord>)inputObj;
      getMapper().map(input.datum(),getMapCollector());
    }
    catch (InterruptedException e)
    {          
      throw new IOException(e);
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
  public void setSchemas(PartitionPreservingSchemas schemas)
  {
    _schemas = schemas;
  }
  
  /**
   * Gets the Avro schemas.
   * 
   * @return schemas
   */
  public PartitionPreservingSchemas getSchemas()
  {
    return _schemas;
  }
  
  @Override
  public void setContext(
      TaskInputOutputContext<Object, Object, Object, Object> context) 
  {
    super.setContext(context);
        
    if (_mapper instanceof Configurable)
    {
      ((Configurable)_mapper).setConf(context.getConfiguration());
    }
  }

  private MapCollector getMapCollector()
  {
    if (_mapCollector == null)
    {
      _mapCollector = new MapCollector(getSchemas());
    }
    
    return _mapCollector;
  }
  
  /**
   * A {@see KeyValueCollector} that writes to {@see MapContext} and tags each mapped key with the time for the partition
   * it was derived from.  This keeps the data partitioned so that the reducer may process each partition independently.
   * 
   * @author "Matthew Hayes"
   *
   */
  private class MapCollector implements KeyValueCollector<GenericRecord,GenericRecord>
  {
    private MapContext<Object,Object,Object,Object> context;
    private GenericRecord wrappedKey;
      
    public MapCollector(PartitionPreservingSchemas schemas)
    {
      this.wrappedKey = new GenericData.Record(schemas.getMapOutputKeySchema());
    }
    
    public void setContext(MapContext<Object,Object,Object,Object> context)
    {
      this.context = context;
    }
    
    public void setTime(long time)
    {
      this.wrappedKey.put("time", time);
    }
    
    public void collect(GenericRecord key, GenericRecord value) throws IOException, InterruptedException
    {        
      wrappedKey.put("value", key);  
      context.write(new AvroKey<GenericRecord>(wrappedKey),new AvroValue<GenericRecord>(value));
    }
  }
}
