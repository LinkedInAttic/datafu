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

/**
 * Remove duplicate tuples in a bag. Null values are supported. 
 * <p>
 * Output tuples may NOT be returned in the same order.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define BagDistinct datafu.pig.bags.sets.BagDistinct();
 * 
 * -- input:
 * -- ({(1),(1),(2),(),()})
 * input = LOAD 'input' AS (B: bag {T: tuple(v:INT)});
 * 
 * -- output:
 * -- ({(), (1), (2)})
 * output = FOREACH input GENERATE BagDistinct(B);
 * }
 * </pre>
 */
public class BagDistinct extends EvalFunc<DataBag>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    if (input.size() == 0 || input.get(0) == null)
      return BagFactory.getInstance().newDefaultBag();
    Object o = input.get(0);
    if (o instanceof DataBag) {
      DataBag outputBag = bagFactory.newDistinctBag();
      DataBag inputBag = (DataBag)o;
      outputBag.addAll(inputBag);
      return outputBag;
    } else {
      throw new IllegalArgumentException("expected a null or a bag");
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                          input.getField(0).schema, DataType.BAG));
    }
    catch (Exception e) {
      return null;
    }
  }
}

