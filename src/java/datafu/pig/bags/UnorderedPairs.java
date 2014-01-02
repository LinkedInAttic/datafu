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
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;

/**
 * Generates pairs of all items in a bag.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define UnorderedPairs datafu.pig.bags.UnorderedPairs();
 * 
 * -- input:
 * -- ({(1),(2),(3),(4)})
 * input = LOAD 'input' AS (B: bag {T: tuple(v:INT)});
 * 
 * -- output:
 * -- ({((1),(2)),((1),(3)),((1),(4)),((2),(3)),((2),(4)),((3),(4))})
 * output = FOREACH input GENERATE UnorderedPairs(B);
 * } 
 * </pre>
 * </p>
 */
public class UnorderedPairs extends EvalFunc<DataBag>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    PigStatusReporter reporter = PigStatusReporter.getInstance();

    try {
      DataBag inputBag = (DataBag) input.get(0);
      DataBag outputBag = bagFactory.newDefaultBag();
      long i=0, j, cnt=0;

      if (inputBag != null)
      {
        for (Tuple elem1 : inputBag) {
          j = 0; 
          for (Tuple elem2 : inputBag) {
            if (j > i) {
              outputBag.add(tupleFactory.newTuple(Arrays.asList(elem1, elem2)));
              cnt++;
            }
            j++;
  
            if (reporter != null)
              reporter.progress();
  
            if (cnt % 1000000 == 0) {
              outputBag.spill();
              cnt = 0;
            }
          }
          i++;
        }
      }
      
      return outputBag;
    }
    catch (Exception e) {
      throw new RuntimeException("Caught exception processing input of " + this.getClass().getName(), e);
    }
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
      
      Schema ouputTupleSchema = new Schema();
      ouputTupleSchema.add(new Schema.FieldSchema("elem1", inputBagSchema.getField(0).schema.clone(), DataType.TUPLE));
      ouputTupleSchema.add(new Schema.FieldSchema("elem2", inputBagSchema.getField(0).schema.clone(), DataType.TUPLE));
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               ouputTupleSchema, 
                                               DataType.BAG));
    }
    catch (Exception e) {
      return null;
    }
  }
}

