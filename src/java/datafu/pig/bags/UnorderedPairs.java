/*
 * Copyright 2010 LinkedIn, Inc
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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.google.common.collect.ImmutableList;

/**
 * Generates pairs of all items in a bag.
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
              outputBag.add(tupleFactory.newTuple(ImmutableList.of(elem1, elem2)));
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
      throw WrappedIOException.wrap("Caught exception processing input of " + this.getClass().getName(), e);
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("elem1", input.getField(0).schema));
      tupleSchema.add(new Schema.FieldSchema("elem2", input.getField(0).schema));
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
              tupleSchema, DataType.BAG));
    }
    catch (Exception e) {
      return null;
    }
  }
}

