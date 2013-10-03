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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.Pair;
import org.apache.commons.lang.NullArgumentException;

/**
 * Generates the Avro schemas used by {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob} and its derivations.
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitionCollapsingSchemas implements Serializable
{  
  private static String DATED_INTERMEDIATE_VALUE_SCHEMA_NAME = "DatedMapValue";
  private static String KEY_SCHEMA = "key.schema";
  private static String INPUT_SCHEMA = "input.schema";
  private static String INTERMEDIATE_VALUE_SCHEMA = "intermediate.value.schema";
  private static String OUTPUT_VALUE_SCHEMA = "output.value.schema";
  
  private final String _outputSchemaName;
  private final String _outputSchemaNamespace;
  private transient Schema _keySchema;
  private transient Schema _intermediateValueSchema;
  private transient Schema _inputSchema;
  private transient Schema _outputValueSchema;
  
  // generated schemas
  private transient Schema _mapOutputSchema;
  private transient Schema _dateIntermediateValueSchema;
  private transient Schema _mapOutputValueSchema;
  private transient Schema _reduceOutputSchema;
  private transient Schema _mapInputSchema;
  
  //schemas are stored here so the object can be serialized
  private Map<String,String> conf;
  
  public PartitionCollapsingSchemas(TaskSchemas schemas, List<Schema> inputSchemas, String outputSchemaName, String outputSchemaNamespace)
  {
    if (schemas == null)
    {
      throw new NullArgumentException("schemas");
    }
    if (inputSchemas == null)
    {
      throw new NullArgumentException("inputSchema");
    }
    if (outputSchemaName == null)
    {
      throw new NullArgumentException("outputSchemaName");
    }
    if (outputSchemaName == outputSchemaNamespace)
    {
      throw new NullArgumentException("outputSchemaNamespace");
    }
    _outputSchemaName = outputSchemaName;
    _outputSchemaNamespace = outputSchemaNamespace;
    
    conf = new HashMap<String,String>();
    conf.put(INPUT_SCHEMA, Schema.createUnion(inputSchemas).toString());
    conf.put(KEY_SCHEMA, schemas.getKeySchema().toString());
    conf.put(INTERMEDIATE_VALUE_SCHEMA, schemas.getIntermediateValueSchema().toString());
    conf.put(OUTPUT_VALUE_SCHEMA, schemas.getOutputValueSchema().toString());
  }
  
  public Schema getInputSchema()
  {
    if (_inputSchema == null)
    {
      _inputSchema = new Schema.Parser().parse(conf.get(INPUT_SCHEMA));
    }
    return _inputSchema;
  }
  
  public Schema getMapInputSchema()
  {    
    if (_mapInputSchema == null)
    {
      List<Schema> mapInputSchemas = new ArrayList<Schema>();
      
      if (getInputSchema().getType() == Type.UNION)
      {
        mapInputSchemas.addAll(getInputSchema().getTypes());
      }
      else
      {
        mapInputSchemas.add(getInputSchema());
      }
      
      // feedback from output (optional)
      mapInputSchemas.add(getReduceOutputSchema());
      
      _mapInputSchema = Schema.createUnion(mapInputSchemas);
    }
    return _mapInputSchema;
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
    return getKeySchema();
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
    
  public Schema getDatedIntermediateValueSchema()
  {
    if (_dateIntermediateValueSchema == null)
    {
      _dateIntermediateValueSchema = Schema.createRecord(DATED_INTERMEDIATE_VALUE_SCHEMA_NAME, null, _outputSchemaNamespace, false);
      List<Field> intermediateValueFields = Arrays.asList(new Field("value", getIntermediateValueSchema(), null, null),
                                                         new Field("time", Schema.create(Type.LONG), null, null));
      _dateIntermediateValueSchema.setFields(intermediateValueFields);
    }
    return _dateIntermediateValueSchema;
  }
  
  public Schema getOutputValueSchema()
  {
    if (_outputValueSchema == null)
    {
      _outputValueSchema = new Schema.Parser().parse(conf.get(OUTPUT_VALUE_SCHEMA));
    }
    return _outputValueSchema;
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
    if (_mapOutputValueSchema == null)
    {
      List<Schema> unionSchemas = new ArrayList<Schema>();
      
      unionSchemas.add(getIntermediateValueSchema());
      
      // intermediate values tagged with the date
      unionSchemas.add(getDatedIntermediateValueSchema());
      
      // feedback from output of second pass
      if (!unionSchemas.contains(getOutputValueSchema()))
      {
        unionSchemas.add(getOutputValueSchema());
      }
      
      _mapOutputValueSchema = Schema.createUnion(unionSchemas);
    }
    return _mapOutputValueSchema;
  }
}
