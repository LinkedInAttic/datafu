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

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Iterator;


/**
 * Lead is an analytic function like Oracle's Lead function. It provides access to more than one tuple of a bag at
 * the same time without a self join. Given a bag of tuple returned from a query,  LEAD provides access to a tuple at a
 * given physical offset beyond that position. Generates pairs of all items in a bag.
 *
 * If you do not specify offset, then its default is 1. Null is returned if the offset goes beyond the scope of the bag.
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * define Lead datafu.pig.bags.Lead('1');
 *
 * -- input:
 * -- ({(1),(2),(3),(4)})
 * input = LOAD 'input' AS (B: bag {T: tuple(v:INT)});
 *
 * -- output:
 * -- ({((1),(2)),((2),(3)),((3),(4)),((4),)})
 * output = FOREACH input GENERATE Lead(B);
 * }
 * </pre>
 * </p>
 */
public class Lead extends AccumulatorEvalFunc<DataBag> {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  private final int offset;

  private DataBag outputBag;
  private long count;

  public Lead() {
    this("1");
  }

  public Lead(String off) {
    this.offset = Integer.parseInt(off) + 1;
    if(this.offset < 2 ) {
      throw new RuntimeException("In function Lead('n') where n should > 0");
    }
    cleanup();
  }

  @Override
  public void accumulate(Tuple tuple) throws IOException {
    DataBag inputBag = (DataBag) tuple.get(0);
    Object[] predecessors = new Object[offset];
    Iterator<Tuple>  iterator = inputBag.iterator();
    for (long index = 0; index < inputBag.size() + offset; index ++) {
      Tuple t = (index < inputBag.size()) ? iterator.next() : null;
      if(index >= offset) {
        Tuple newTuple = tupleFactory.newTuple();
        for (int i = 0; i < offset; i++) {
          newTuple.append(predecessors[(int) ((index + i) % offset)]);
        }
        outputBag.add(newTuple);
        predecessors[(int) (index % offset)] = t;

        if (count % 1000000 == 0) {
          outputBag.spill();
          count = 0;
        }
        count++;
      } else {
        predecessors[(int)index] = t;
      }
    }
  }

  @Override
  public void cleanup() {
    this.outputBag = BagFactory.getInstance().newDefaultBag();
    this.count = 0;
  }

  @Override
  public DataBag getValue() {
    return outputBag;
  }

  @Override
  public Schema outputSchema(Schema input) {
    if (input.size() != 1) {
      throw new RuntimeException("Expected input to have only a single field");
    }

    try {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG) {
        throw new RuntimeException("Expected a BAG as input");
      }

      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE) {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
            DataType.findTypeName(inputBagSchema.getField(0).type)));
      }

      Schema ouputTupleSchema = new Schema();
      for(int i = 0; i < offset; i++) {
        ouputTupleSchema
            .add(new Schema.FieldSchema("elem" + i, inputBagSchema.getField(0).schema.clone(), DataType.TUPLE));
      }
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
          ouputTupleSchema,
          DataType.BAG));
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}