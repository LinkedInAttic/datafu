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

package datafu.hourglass.schemas;

import org.apache.avro.Schema;

/**
 * Contains the Avro schemas for the key, intermediate value, and output value of a job.
 * 
 * <p>
 * The mapper and combiner output key-value pairs conforming to the key and intermediate
 * value schemas defined here.
 * The reducer outputs key-value pairs conforming to the key and output value schemas.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class TaskSchemas
{    
  private Schema keySchema;
  private Schema intermediateValueSchema;
  private Schema outputValueSchema;
  
  private TaskSchemas(Schema keySchema, Schema intermediateValueSchema, Schema outputValueSchema)
  {    
    if (keySchema == null)
    {
      throw new IllegalArgumentException("missing key schema");
    }
    
    if (intermediateValueSchema == null)
    {
      throw new IllegalArgumentException("missing intermediate value schema");
    }
    
    if (outputValueSchema == null)
    {
      throw new IllegalArgumentException("missing output value schema");
    }
    
    this.keySchema = keySchema;
    this.intermediateValueSchema = intermediateValueSchema;
    this.outputValueSchema = outputValueSchema;
  }
  
  public Schema getKeySchema()
  {
    return keySchema;
  }

  public Schema getIntermediateValueSchema()
  {
    return intermediateValueSchema;
  }

  public Schema getOutputValueSchema()
  {
    return outputValueSchema;
  }
  
  public static class Builder
  {
    private Schema keySchema;
    private Schema intermediateValueSchema;
    private Schema outputValueSchema;
    
    public Builder setKeySchema(Schema schema)
    {
      keySchema = schema;
      return this;
    }
    
    public Builder setIntermediateValueSchema(Schema schema)
    {
      intermediateValueSchema = schema;
      return this;
    }
    
    public Builder setOutputValueSchema(Schema schema)
    {
      outputValueSchema = schema;
      return this;
    }
    
    public TaskSchemas build()
    {
      return new TaskSchemas(keySchema,intermediateValueSchema,outputValueSchema);
    }
  }
}
