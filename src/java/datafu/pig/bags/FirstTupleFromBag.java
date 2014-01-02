/*
 * Copyright 2012 LinkedIn Corp. and contributors
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

import datafu.pig.util.SimpleEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Returns the first tuple from a bag. Requires a second parameter that will be returned if the bag is empty.
 *
 * Example:
 * <pre>
 * {@code
 * define FirstTupleFromBag datafu.pig.bags.FirstTupleFromBag();
 *
 * -- input:
 * -- ({(a,1)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 *
 * output = FOREACH input GENERATE FirstTupleFromBag(B, null);
 *
 * -- output:
 * -- (a,1)
 * }
 * </pre>
 */

public class FirstTupleFromBag extends SimpleEvalFunc<Tuple>
{
  public Tuple call(DataBag bag, Tuple defaultValue) throws IOException
  {
    for (Tuple t : bag) {
      return t;
    }
    return defaultValue;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      return new Schema(input.getField(0).schema);
    }
    catch (Exception e) {
      return null;
    }
  }
}

