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

package datafu.pig.bags;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Unions all input bags to produce a single bag containing all tuples.
 * <p>
 * This UDF accepts two forms of input:
 * <ol>
 *  <li>a tuple of 2 or more elements where each element is a bag with the same schema</li>
 *  <li>a single bag where each element of that bag is a bag and all of these bags have the same schema</li>
 * </ol>
 * <p>
 * Example 1:
 * <pre>
 * {@code
 * define BagConcat datafu.pig.bags.BagConcat();
 * -- This example illustrates the use on a tuple of bags
 * 
 * -- input:
 * -- ({(1),(2),(3)},{(3),(4),(5)})
 * -- ({(20),(25)},{(40),(50)})
 * input = LOAD 'input' AS (A: bag{T: tuple(v:INT)}, B: bag{T: tuple(v:INT)});
 * 
 * -- output:
 * -- ({(1),(2),(3),(3),(4),(5)})
 * -- ({(20),(25),(40),(50)})
 * output = FOREACH input GENERATE BagConcat(A,B); 
 * }
 * </pre>
 * <p>
 * Example 2:
 * <pre>
 * {@code
 * define BagConcat datafu.pig.bags.BagConcat();
 * -- This example illustrates the use on a bag of bags
 * 
 * -- input:
 * -- ({({(1),(2),(3)}),({(3),(4),(5)})})
 * -- ({({(20),(25)}),({(40),(50)})})
 * input = LOAD 'input' AS (A: bag{T: tuple(bag{T2: tuple(v:INT)})});
 * 
 * -- output:
 * -- ({(1),(2),(3),(3),(4),(5)})
 * -- ({(20),(25),(40),(50)})
 * output = FOREACH input GENERATE BagConcat(A);
 * }
 * </pre>
 *  
 * @author wvaughan
 *
 */
public class BagConcat extends EvalFunc<DataBag>
{

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    DataBag output = BagFactory.getInstance().newDefaultBag();
    if (input.size() > 1) {
      // tuple of bags
      for (int i=0; i < input.size(); i++) {
        Object o = input.get(i);
        if (!(o instanceof DataBag)) {
          throw new RuntimeException("Expected a TUPLE of BAGs as input");
        }

        DataBag inputBag = (DataBag) o;
        for (Tuple elem : inputBag) {
          output.add(elem);
        }
      }
    } else {
      // bag of bags
      DataBag outerBag = (DataBag)input.get(0);
      for (Tuple outerTuple : outerBag) {
        DataBag innerBag = (DataBag)outerTuple.get(0);
        for (Tuple innerTuple : innerBag) {
          output.add(innerTuple);
        }
      }
    }
    return output;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      Schema outputBagTupleSchema = null;
      
      if (input.size() == 0) {
        throw new RuntimeException("Expected input tuple to contain fields. Got none.");
      }
      // determine whether the input is a tuple of bags or a bag of bags      
      if (input.size() != 1) {
        // tuple of bags
        
        // verify that each element in the input is a bag
        for (FieldSchema fieldSchema : input.getFields()) {
          if (fieldSchema.type != DataType.BAG) {
            throwBadTypeError("Expected a TUPLE of BAGs as input.  Got instead: %s in schema: %s", fieldSchema.type, input);
          }
          if (fieldSchema.schema == null) {
            throwBadSchemaError(fieldSchema.alias, input);
          }
        }
        
        outputBagTupleSchema = input.getField(0).schema;                
      } else {
        // bag of bags
        
        // should only be a single element in the input and it should be a bag
        FieldSchema fieldSchema = input.getField(0); 
        if (fieldSchema.type != DataType.BAG) {
          throwBadTypeError("Expected a BAG of BAGs as input.  Got instead: %s in schema: %s", fieldSchema.type, input);
        }
        
        // take the tuple schema from this outer bag
        Schema outerBagTuple = input.getField(0).schema;      
        // verify that this tuple contains only a bag for each element
        Schema outerBagSchema = outerBagTuple.getField(0).schema;      
        if (outerBagSchema.size() != 1) {
          throw new RuntimeException(String.format("Expected outer bag to have only a single field.  Got instead: %s", 
                                                   outerBagSchema.prettyPrint()));
        }
        FieldSchema outerBagFieldSchema = outerBagSchema.getField(0); 
        if (outerBagFieldSchema.type != DataType.BAG) {
          throwBadTypeError("Expected a BAG of BAGs as input.  Got instead: %s", outerBagFieldSchema.type, outerBagTuple); 
        }
        
        // take the schema of the inner tuple as the schema for the tuple in the output bag
        FieldSchema innerTupleSchema = outerBagSchema.getField(0);        
        if (innerTupleSchema.schema == null) {
          throwBadSchemaError(innerTupleSchema.alias, outerBagSchema);
        }
        outputBagTupleSchema = innerTupleSchema.schema;
      }    
      
      // return the correct schema
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                      outputBagTupleSchema, DataType.BAG));      
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void throwBadSchemaError(String alias, Schema schema) throws FrontendException
  {    
    throw new FrontendException(String.format("Expected non-null schema for all bags. Got null on field %s, in: %s", 
                                              alias, schema.prettyPrint()));    
  }

  private void throwBadTypeError(String expectedMessage, byte actualType, Schema schema) throws FrontendException
  {
    throw new FrontendException(String.format(expectedMessage, DataType.findTypeName(actualType), schema.prettyPrint()));
  }
}
