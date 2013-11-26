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
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Generates a count of the number of times each distinct tuple appears in a bag.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE CountEach datafu.pig.bags.CountEach();
 * DEFINE CountEachFlatten datafu.pig.bags.CountEach('flatten');
 * 
 * -- input: 
 * -- ({(A),(A),(C),(B)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 * 
 * -- output: 
 * -- {((A),2),((C),1),((B),1)}
 * output = FOREACH input GENERATE CountEach(B); 
 * 
 * -- output_flatten: 
 * -- ({(A,2),(C,1),(B,1)})
 * output_flatten = FOREACH input GENERATE CountEachFlatten(B);
 * } 
 * </pre>
 * </p>
 */
public class CountEach extends AccumulatorEvalFunc<DataBag>
{
  private boolean flatten = false;
  private Map<Tuple, Integer> counts = new HashMap<Tuple, Integer>();
  
  public CountEach() {
    
  }
  
  public CountEach(String arg) {
    if (arg != null && arg.toLowerCase().equals("flatten")) {
      flatten = true;
    }
  }

  @Override
  public void accumulate(Tuple input) throws IOException
  {
    DataBag inputBag = (DataBag)input.get(0);
    if (inputBag == null) throw new IllegalArgumentException("Expected a bag, got null");
    
    for (Tuple tuple : inputBag) {
      if (!counts.containsKey(tuple)) {
        counts.put(tuple, 0);
      }
      counts.put(tuple, counts.get(tuple)+1);
    }
  }

  @Override
  public DataBag getValue()
  {
    DataBag output = BagFactory.getInstance().newDefaultBag();
    for (Tuple tuple : counts.keySet()) {
      Tuple outputTuple = null;
      Tuple innerTuple = TupleFactory.getInstance().newTuple(tuple.getAll());
      if (flatten) {        
        innerTuple.append(counts.get(tuple));
        outputTuple = innerTuple;
      } else {
        outputTuple = TupleFactory.getInstance().newTuple();
        outputTuple.append(innerTuple);
        outputTuple.append(counts.get(tuple));
      }
      output.add(outputTuple);
    }

    return output;
  }

  @Override
  public void cleanup()
  {
    counts.clear();
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have one field");
      }
      
      Schema.FieldSchema bagFieldSchema = input.getField(0);

      if (bagFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      Schema inputBagSchema = bagFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }      
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      if (inputTupleSchema == null) inputTupleSchema = new Schema();
      
      Schema outputTupleSchema = null;
      
      if (this.flatten) {
        outputTupleSchema = inputTupleSchema.clone();
        outputTupleSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
      } else {        
        outputTupleSchema = new Schema();
        outputTupleSchema.add(new Schema.FieldSchema("tuple_schema", inputTupleSchema.clone(), DataType.TUPLE));
        outputTupleSchema.add(new Schema.FieldSchema("count", DataType.INTEGER));
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
