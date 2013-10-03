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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;

/**
 * Generates the Avro schemas used by {@link datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob} and its derivations.
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitionPreservingSchemas implements Serializable
{  
  private static String DATED_INCREMENTAL_KEY_SCHEMA_NAME = "DatedMapKey";
  
  private static String KEY_SCHEMA = "key.schema";
  private static String INTERMEDIATE_VALUE_SCHEMA = "intermediate.value.schema";
  private static String INPUT_SCHEMA = "input.schema";
  private static String OUPUT_VALUE_SCHEMA = "output.value.schema";
  
  private transient Schema _keySchema;
  private transient Schema _intermediateValueSchema;
  private transient Schema _inputSchema;
  private transient Schema _outputValueSchema;
  private final String _outputSchemaName;
  private final String _outputSchemaNamespace;
  
  // generated schemas
  private transient Schema _reduceOutputSchema;
  private transient Schema _mapOutputKeySchema;
  private transient Schema _mapOutputSchema;
  
  // schemas are stored here so the object can be serialized
  private Map<String,String> conf;
  
  public PartitionPreservingSchemas(TaskSchemas schemas, List<Schema> inputSchemas, String outputSchemaName, String outputSchemaNamespace)
  {
    _outputSchemaName = outputSchemaName;
    _outputSchemaNamespace = outputSchemaNamespace;
    
    conf = new HashMap<String,String>();
    conf.put(KEY_SCHEMA, schemas.getKeySchema().toString());
    conf.put(INTERMEDIATE_VALUE_SCHEMA, schemas.getIntermediateValueSchema().toString());
    conf.put(INPUT_SCHEMA, Schema.createUnion(inputSchemas).toString());
    conf.put(OUPUT_VALUE_SCHEMA, schemas.getOutputValueSchema().toString());
  }
  
  public Schema getMapInputSchema()
  {
    if (_inputSchema == null)
    {
      _inputSchema = new Schema.Parser().parse(conf.get(INPUT_SCHEMA));
    }
    return _inputSchema;
  }
    
  public Schema getMapOutputSchema()
  {
    if (_mapOutputSchema == null)
    {
      _mapOutputSchema = Pair.getPairSchema(getMapOutputKeySchema(), 
                                            getMapOutputValueSchema());
    }
    return _mapOutputSchema;
  }
  
  public Schema getReduceOutputSchema()
  {
    if (_reduceOutputSchema == null)
    {
      _reduceOutputSchema = Schema.createRecord(_outputSchemaName, null, _outputSchemaNamespace, false);            
      List<Field> fields = Arrays.asList(new Field("key",getKeySchema(), null, null),
                                         new Field("value", getOutputValueSchema(), null, null));    
      _reduceOutputSchema.setFields(fields);
    }
    return _reduceOutputSchema;
  }
  
  public Schema getOutputValueSchema()
  {
    if (_outputValueSchema == null)
    {
      _outputValueSchema = new Schema.Parser().parse(conf.get(OUPUT_VALUE_SCHEMA));
    }
    return _outputValueSchema;
  }
  
  public Schema getKeySchema()
  {
    if (_keySchema == null)
    {
      _keySchema = new Schema.Parser().parse(conf.get(KEY_SCHEMA));
    }
    return _keySchema;
  }
        
  public Schema getMapOutputKeySchema()
  {
    if (_mapOutputKeySchema == null)
    {
        _mapOutputKeySchema = Schema.createRecord(DATED_INCREMENTAL_KEY_SCHEMA_NAME, null, _outputSchemaNamespace, false);
      // key needs the time included with it for partitioning
      List<Field> incrementalKeyFields = Arrays.asList(new Field("value", getKeySchema(), null, null),
                                                         new Field("time", Schema.create(Type.LONG), null, null));
      _mapOutputKeySchema.setFields(incrementalKeyFields);
    }
    return _mapOutputKeySchema;
  }
  
  public Schema getIntermediateValueSchema()
  {
    if (_intermediateValueSchema == null)
    {
      _intermediateValueSchema = new Schema.Parser().parse(conf.get(INTERMEDIATE_VALUE_SCHEMA));
    }
    return _intermediateValueSchema;
  }
    
  public Schema getMapOutputValueSchema()
  {    
    return getIntermediateValueSchema();
  }
}