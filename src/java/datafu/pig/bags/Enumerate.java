/*
 * Copyright 2010 LinkedIn Corp. and contributors
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

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Enumerate a bag, appending to each tuple its index within the bag.
 * 
 * <p>
 * For example:
 * <pre>
 *   {(A),(B),(C),(D)} => {(A,0),(B,1),(C,2),(D,3)}
 * </pre>
 * The first constructor parameter (optional) dictates the starting index of the counting.
 * This UDF implements the accumulator interface, reducing DataBag materialization costs.
 * </p>
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * define Enumerate datafu.pig.bags.Enumerate('1');
 *
 * -- input:
 * -- ({(100),(200),(300),(400)})
 * input = LOAD 'input' as (B: bag{T: tuple(v2:INT)});
 *
 * -- output:
 * -- ({(100,1),(200,2),(300,3),(400,4)})
 * output = FOREACH input GENERATE Enumerate(B);
 * }
 * </pre>
 */
public class Enumerate extends AccumulatorEvalFunc<DataBag>
{
  private final int start;
  
  private DataBag outputBag;
  private long i;
  private long count;

  public Enumerate()
  {
    this("0");
  }

  public Enumerate(String start)
  {
    this.start = Integer.parseInt(start);
    cleanup();
  }
  
  @Override
  public void accumulate(Tuple arg0) throws IOException
  {
    DataBag inputBag = (DataBag)arg0.get(0);
    for (Tuple t : inputBag) {
      Tuple t1 = TupleFactory.getInstance().newTuple(t.getAll());
      t1.append(i);
      outputBag.add(t1);

      if (count % 1000000 == 0) {
        outputBag.spill();
        count = 0;
      }
      i++;
      count++;
    }
  }

  @Override
  public void cleanup()
  {
    this.outputBag = BagFactory.getInstance().newDefaultBag();
    this.i = this.start;
    this.count = 0;
  }

  @Override
  public DataBag getValue()
  {
    return outputBag;
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
      outputTupleSchema.add(new Schema.FieldSchema("i", DataType.LONG));
      
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
