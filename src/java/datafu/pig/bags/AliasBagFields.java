/*
 * Copyright 2011 LinkedIn, Inc
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

package datafu.pig.bags;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Re-alias the fields inside of a bag.  For example:
 * 
 * Example:
 * <pre>
 * {@code
 * define AliasBagFields datafu.pig.bags.AliasBagFields('[alpha#letter,numeric#decimal]');
 * 
 * -- input:
 * -- ({(a, 1),(b, 2),(c, 3),(d, 4)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 * 
 * output = FOREACH input GENERATE AliasBagFields(B);
 * 
 * output schema => (B: bag {T: tuple(letter:CHARARRAY, decimal:INT)});
 * } 
 * </pre>
 * 
 * @param map A string in Pig map format [key1#value1,key2#value2]
 */
public class AliasBagFields extends SimpleEvalFunc<DataBag>
{
  private final HashMap<String, String> aliasMap = new HashMap<String, String>();
  
  public AliasBagFields(String map)
  {
    if (map == null || map.length() < 2) {
      throw new RuntimeException("Malformed map string");
    } else {
      String fieldString = map.substring(1, map.length()-1);
      for (String pair : fieldString.split(",")) {
        String[] tokens = pair.split("#");
        if (tokens.length != 2) {
          throw new RuntimeException("Malformed map string");
        } else {
          aliasMap.put(tokens[0].replaceAll(" ", ""), tokens[1].replaceAll(" ", ""));
        }
      }
    }
  }
  
  public DataBag call(DataBag inputBag) throws IOException
  {
    return inputBag;
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have only a single field");
      }
      
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      Schema outputTupleSchema = inputTupleSchema.clone();
      for (FieldSchema fieldSchema : outputTupleSchema.getFields()) {
        if (aliasMap.containsKey(fieldSchema.alias)) {
          fieldSchema.alias = aliasMap.get(fieldSchema.alias);
        }
      }
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
