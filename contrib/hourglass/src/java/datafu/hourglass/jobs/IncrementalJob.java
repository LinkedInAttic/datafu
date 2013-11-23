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

import java.util.Properties;

import org.apache.avro.Schema;

import datafu.hourglass.schemas.TaskSchemas;

/**
 * Base class for incremental jobs.  Incremental jobs consume day-partitioned input data.  
 * 
 * <p>
 * Implementations of this class must provide key, intermediate value, and output value schemas.
 * The key and intermediate value schemas define the output for the mapper and combiner.
 * The key and output value schemas define the output for the reducer.
 * </p>
 * 
 * <p>
 * This class has the same configuration and methods as {@link TimeBasedJob}.
 * In addition it also recognizes the following properties:
 * </p>
 * 
 * <ul>
 *   <li><em>max.iterations</em> - maximum number of iterations for the job</li>
 *   <li><em>max.days.to.process</em> - maximum number of days of input data to process in a single run</li>
 *   <li><em>fail.on.missing</em> - whether the job should fail if input data within the desired range is missing</li>
 * </ul>
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class IncrementalJob extends TimeBasedJob
{
  private Integer _maxToProcess;
  private Integer _maxIterations;
  private boolean _failOnMissing;
  private TaskSchemas _schemas;
  
  /**
   * Initializes the job.
   */
  public IncrementalJob()
  {    
  }

  /**
   * Initializes the job with a job name and properties.
   * 
   * @param name job name
   * @param props configuration properties
   */
  public IncrementalJob(String name, Properties props)
  {        
    super(name,props);
  }
  
  public void setProperties(Properties props)
  {
    super.setProperties(props);
        
    if (getProperties().get("max.iterations") != null)
    {
      setMaxIterations(Integer.parseInt((String)getProperties().get("max.iterations")));
    }
    
    if (getProperties().get("max.days.to.process") != null)
    {
      setMaxToProcess(Integer.parseInt((String)getProperties().get("max.days.to.process")));
    }
    
    if (getProperties().get("fail.on.missing") != null)
    {
      setFailOnMissing(Boolean.parseBoolean((String)getProperties().get("max.days.to.process")));
    }
  }
  
  protected void initialize()
  {
    super.initialize();
    
    if (getKeySchema() == null)
    {
      throw new RuntimeException("Key schema not specified");
    }

    if (getIntermediateValueSchema() == null)
    {
      throw new RuntimeException("Intermediate schema not specified");
    }

    if (getOutputValueSchema() == null)
    {
      throw new RuntimeException("Output schema not specified");
    }
    
    _schemas = new TaskSchemas.Builder()
      .setKeySchema(getKeySchema())
      .setIntermediateValueSchema(getIntermediateValueSchema())
      .setOutputValueSchema(getOutputValueSchema())
      .build();
  }
  
  /**
   * Gets the Avro schema for the key.
   * <p>
   * This is also used as the key for the map output.
   * 
   * @return key schema.
   */
  protected abstract Schema getKeySchema();
  
  /**
   * Gets the Avro schema for the intermediate value.
   * <p>
   * This is also used for the value for the map output.
   * 
   * @return intermediate value schema
   */
  protected abstract Schema getIntermediateValueSchema();
  
  /**
   * Gets the Avro schema for the output data.
   * 
   * @return output data schema
   */
  protected abstract Schema getOutputValueSchema();
  
  /**
   * Gets the schemas.
   * 
   * @return schemas
   */
  protected TaskSchemas getSchemas()
  {
    return _schemas;
  }
  
  /**
   * Gets the maximum number of days of input data to process in a single run.
   * 
   * @return maximum number of days to process
   */
  public Integer getMaxToProcess()
  {
    return _maxToProcess;
  }

  /**
   * Sets the maximum number of days of input data to process in a single run.
   * 
   * @param maxToProcess maximum number of days to process
   */
  public void setMaxToProcess(Integer maxToProcess)
  {
    _maxToProcess = maxToProcess;
  }

  /**
   * Gets the maximum number of iterations for the job.  Multiple iterations will only occur
   * when there is a maximum set for the number of days to process in a single run.
   * An error should be thrown if this number will be exceeded.
   * 
   * @return maximum number of iterations
   */
  public Integer getMaxIterations()
  {
    return _maxIterations;
  }

  /**
   * Sets the maximum number of iterations for the job.  Multiple iterations will only occur
   * when there is a maximum set for the number of days to process in a single run.
   * An error should be thrown if this number will be exceeded.
   * 
   * @param maxIterations maximum number of iterations
   */
  public void setMaxIterations(Integer maxIterations)
  {
    _maxIterations = maxIterations;
  }

  /**
   * Gets whether the job should fail if input data within the desired range is missing. 
   * 
   * @return true if the job should fail on missing data
   */
  public boolean isFailOnMissing()
  {
    return _failOnMissing;
  }

  /**
   * Sets whether the job should fail if input data within the desired range is missing. 
   * 
   * @param failOnMissing true if the job should fail on missing data
   */
  public void setFailOnMissing(boolean failOnMissing)
  {
    _failOnMissing = failOnMissing;
  }
}
