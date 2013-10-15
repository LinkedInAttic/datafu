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

package datafu.hourglass.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Helper methods for dealing with multiple Avro input schemas.  A mapping is stored in the configuration
 * that maps each input path to its corresponding schema.  Methods in this class help with loading and
 * storing these schema mappings.
 * 
 * @author "Matthew Hayes"
 *
 */
public class AvroMultipleInputsUtil
{
  private static final String CONF_INPUT_KEY_SCHEMA = "avro.schema.multiple.inputs.keys";
  
  /**
   * Gets the schema for a particular input split. 
   * 
   * @param conf configuration to get schema from
   * @param split input split to get schema for
   * @return schema
   */
  public static Schema getInputKeySchemaForSplit(Configuration conf, InputSplit split) 
  {
    String path = ((FileSplit)split).getPath().toString();
    JSONObject schemas;
    try
    {
      schemas = getInputSchemas(conf);
    }
    catch (JSONException e1)
    {
      throw new RuntimeException(e1);
    }
    String schemaString = null;
    if (schemas != null)
    {
      for (String key : JSONObject.getNames(schemas))
      {
        if (path.startsWith(key))
        {
          try
          {
            schemaString = schemas.getString(key);
            break;
          }
          catch (JSONException e)
          {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return schemaString != null ? new Schema.Parser().parse(schemaString) : null;
  }
  
  /**
   * Sets the job input key schema for a path.
   *
   * @param job The job to configure.
   * @param schema The input key schema.
   * @param path the path to set the schema for
   */
  public static void setInputKeySchemaForPath(Job job, Schema schema, String path) 
  { 
    JSONObject schemas;    
    try
    {
      schemas = getInputSchemas(job.getConfiguration());
      schemas.put(path, schema.toString());
    }
    catch (JSONException e)
    {
      throw new RuntimeException(e);
    }         
    job.getConfiguration().set(CONF_INPUT_KEY_SCHEMA, schemas.toString());
  }
  
  /**
   * Get a mapping from path to input schema.
   * 
   * @param conf
   * @return mapping from path to input schem
   * @throws JSONException
   */
  private static JSONObject getInputSchemas(Configuration conf) throws JSONException
  {
    JSONObject schemas;
    
    String schemasJson = conf.get(CONF_INPUT_KEY_SCHEMA);
    
    if (schemasJson == null)
    {
      schemas = new JSONObject();
    }
    else
    {
      schemas = new JSONObject(schemasJson);
    }   
    
    return schemas;
  }
}
