/*
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

package datafu.pig.util;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Returns the first non-null value from a tuple, just like {@link <a href="http://msdn.microsoft.com/en-us/library/ms190349.aspx" target="_blank">COALESCE</a>} in SQL. 
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 *
 * define COALESCE datafu.pig.util.COALESCE();

 * -- input: 1,2,3,NULL,4,NULL,5
 * input = LOAD 'input' AS (val:int);
 *
 * -- produces: 1,2,3,99,4,99,5
 * coalesced = FOREACH input GENERATE COALESCE(val,99);
 *
 * }
 * </pre>
 * </p>
 * 
 * @author "Matthew Hayes <mhayes@linkedin.com>"
 *
 */
public class Coalesce extends EvalFunc<Object>
{
  @Override
  public Object exec(Tuple input) throws IOException
  {
    
    if (input == null || input.size() == 0)
    {
      return null;
    }
        
    for (Object o : input)
    {
      if (o != null)
      {
        return o;
      }
    }
    
    return null;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    if (input.getFields().size() == 0)
    {
      throw new RuntimeException("Expected at least one parameter");
    }
        
    Byte firstType = null;
    int pos = 0;
    for (FieldSchema field : input.getFields())
    {
      if (DataType.isSchemaType(field.type))
      {
        throw new RuntimeException(String.format("Not supported on schema types.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (DataType.isComplex(field.type))
      {
        throw new RuntimeException(String.format("Not supported on complex types.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (!DataType.isUsableType(field.type))
      {
        throw new RuntimeException(String.format("Not a usable type.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (firstType == null)
      {
        firstType = field.type;
      }
      else if (!firstType.equals(field.type))
      {
        throw new RuntimeException(String.format("Expected all types to be equal, but found %s in position %d.  First element has type %s.",
                                                 DataType.findTypeName(field.type),pos,DataType.findTypeName((byte)firstType)));
      }
      
      pos++;
    }
    
    return new Schema(new Schema.FieldSchema("item",firstType));
  }
}
