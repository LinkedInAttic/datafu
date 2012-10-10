/*
 * Copyright 2012 LinkedIn, Inc
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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

/* Given a bag, return the first tuple from the bag

For example,
FirstTupleFromBag({(1), (2)}, null) -> (1)
 */

public class FirstTupleFromBag extends EvalFunc<Object>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override
  public Object exec(Tuple input) throws IOException
  {
    // PigStatusReporter reporter = PigStatusReporter.getInstance();
    try {
      DataBag outputBag = bagFactory.newDefaultBag();
      long i=0, j, cnt=0;
      DataBag inputBag = (DataBag) input.get(0);
      Object default_val = input.get(1);
      for(Tuple bag2tuple : inputBag){
        outputBag.add(bag2tuple);
        return bag2tuple;
      }
      return default_val;
    }
    catch (Exception e) {
      throw WrappedIOException.wrap("Caught exception processing input of " + this.getClass().getName(), e);
    }
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

