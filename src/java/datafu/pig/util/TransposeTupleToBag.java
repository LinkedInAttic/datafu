/*
 * Copyright 2013 LinkedIn Corp. and contributors
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
import java.util.HashMap;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Performs a transpose on a tuple, resulting in a bag of key, value fields where
 * the key is the column name and the value is the value of that column in the tuple.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 *
 * define TransposeTupleToBag datafu.pig.util.TransposeTupleToBag();

 * -- input: 1,10,11,12
 * input = LOAD 'input' AS (id:int,val1:int,val2:int,val3:int);
 *
 * -- produces: 1,{("val1",10),("val2",11),("val3",12)}
 * output = FOREACH input GENERATE id, TransposeTupleToBag(val1 .. val3);
 *
 * }
 * </pre>
 * </p>
 * 
 * @author "William Vaughan <wvaughan@linkedin.com>"
 *
 */
public class TransposeTupleToBag extends AliasableEvalFunc<DataBag>
{
  private final String TRANSPOSE_TYPE = "TRANSPOSE_TYPE";

  @Override
  public Schema getOutputSchema(Schema input)
  {
    try
    {
      // require that every field in the input has the same type
      Byte type = null;
      for (FieldSchema fieldSchema : input.getFields()) {
        if (type == null) {
          type = fieldSchema.type;
        } else {
          if (type != fieldSchema.type) {
            throw new RuntimeException(
                                       String.format("Expected all input types to match.  Got both %s and %s.", 
                                                     DataType.findTypeName(type.byteValue()), DataType.findTypeName(fieldSchema.type)));
          }
        }      
      }
      getInstanceProperties().put(TRANSPOSE_TYPE, type);
      
      Schema outputTupleSchema = new Schema();
      outputTupleSchema.add(new Schema.FieldSchema("key", DataType.CHARARRAY));
      outputTupleSchema.add(new Schema.FieldSchema("value", type));
      return new Schema(new Schema.FieldSchema(
                                               getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               outputTupleSchema, 
                                               DataType.BAG));
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    // initialize a reverse mapping
    HashMap<Integer, String> positionToAlias = new HashMap<Integer, String>();
    for (String alias : getFieldAliases().keySet()) {
      positionToAlias.put(getFieldAliases().get(alias), alias);
    }
    DataBag output = BagFactory.getInstance().newDefaultBag();
    for (int i=0; i<input.size(); i++) {
      Tuple tuple = TupleFactory.getInstance().newTuple();
      tuple.append(positionToAlias.get(i));
      tuple.append(input.get(i));
      output.add(tuple);
    }
    return output;
  }

}
