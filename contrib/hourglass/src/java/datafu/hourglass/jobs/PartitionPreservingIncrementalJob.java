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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.Mapper;

/**
 * A concrete version of {@link AbstractPartitionPreservingIncrementalJob}.
 * 
 * This provides an alternative to extending {@link AbstractPartitionPreservingIncrementalJob}.
 * Instead of extending this class and implementing the abstract methods, this concrete version
 * can be used instead.  Getters and setters have been provided for the abstract methods. 
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitionPreservingIncrementalJob extends AbstractPartitionPreservingIncrementalJob
{
  private Mapper<GenericRecord,GenericRecord,GenericRecord> _mapper;
  private Accumulator<GenericRecord,GenericRecord> _combiner;
  private Accumulator<GenericRecord,GenericRecord> _reducer;
  private Schema _keySchema;
  private Schema _intermediateValueSchema;
  private Schema _outputValueSchema;
  private Setup _setup;
  
  /**
   * Initializes the job.  The job name is derived from the name of a provided class.
   * 
   * @param cls class to base job name on
   * @throws IOException
   */
  public PartitionPreservingIncrementalJob(@SuppressWarnings("rawtypes") Class cls) throws IOException
  {
    setName(cls.getName());
  }
  
  @Override
  public Mapper<GenericRecord,GenericRecord,GenericRecord> getMapper()
  {
    return _mapper;
  }

  @Override
  public Accumulator<GenericRecord,GenericRecord> getCombinerAccumulator()
  {
    return _combiner;
  }
  
  @Override
  public Accumulator<GenericRecord,GenericRecord> getReducerAccumulator()
  {
    return _reducer;
  }

  @Override
  protected Schema getKeySchema()
  {
    return _keySchema;
  }

  @Override
  protected Schema getIntermediateValueSchema()
  {
    return _intermediateValueSchema;
  }

  @Override
  protected Schema getOutputValueSchema()
  {
    return _outputValueSchema;
  }

  /**
   * Set the mapper.
   * 
   * @param mapper
   */
  public void setMapper(Mapper<GenericRecord,GenericRecord,GenericRecord> mapper)
  {
    this._mapper = mapper;
  }

  /**
   * Set the accumulator for the combiner
   * 
   * @param combiner accumulator for the combiner
   */
  public void setCombinerAccumulator(Accumulator<GenericRecord,GenericRecord> combiner)
  {
    this._combiner = combiner;
  }

  /**
   * Set the accumulator for the reducer.
   * 
   * @param reducer accumulator for the reducer
   */
  public void setReducerAccumulator(Accumulator<GenericRecord,GenericRecord> reducer)
  {
    this._reducer = reducer;
  }

  /**
   * Sets the Avro schema for the key.
   * <p>
   * This is also used as the key for the map output.
   * 
   * @param keySchema key schema
   */
  public void setKeySchema(Schema keySchema)
  {
    this._keySchema = keySchema;
  }

  /**
   * Sets the Avro schema for the intermediate value.
   * <p>
   * This is also used for the value for the map output.
   * 
   * @param intermediateValueSchema intermediate value schema
   */
  public void setIntermediateValueSchema(Schema intermediateValueSchema)
  {
    this._intermediateValueSchema = intermediateValueSchema;
  }

  /**
   * Sets the Avro schema for the output data.
   *  
   * @param outputValueSchema output value schema
   */
  public void setOutputValueSchema(Schema outputValueSchema)
  {
    this._outputValueSchema = outputValueSchema;
  }
  
  /**
   * Set callback to provide custom configuration before job begins execution.
   * 
   * @param setup object with callback method
   */
  public void setOnSetup(Setup setup)
  {
    _setup = setup;
  }
  
  @Override
  public void config(Configuration conf)
  {    
    super.config(conf);
    if (_setup != null)
    {
      _setup.setup(conf);
    }
  }
}
