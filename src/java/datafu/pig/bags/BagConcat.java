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
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Concatenates the tuples from a set of bags, producing a single bag containing all tuples.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define BagConcat datafu.pig.bags.BagConcat();
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
 */
public class BagConcat extends EvalFunc<DataBag>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    DataBag outputBag = bagFactory.newDefaultBag();

    try {
      for (int i=0; i < input.size(); i++) {
        Object o = input.get(i);
        if (!(o instanceof DataBag))
          throw new RuntimeException("parameters must be databags");

        DataBag inputBag = (DataBag) o;
        for (Tuple elem : inputBag) 
          outputBag.add(elem);
      }

      return outputBag;
    }
    catch (Exception e) {
      throw new IOException(e);
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

