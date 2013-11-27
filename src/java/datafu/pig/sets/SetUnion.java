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
 
package datafu.pig.sets;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Computes the set union of two or more bags.  Duplicates are eliminated.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SetUnion datafu.pig.sets.SetUnion();
 * 
 * -- input:
 * -- ({(2,20),(3,30),(4,40)},{(1,10),(2,20),(4,40),(8,80)})
 * input = LOAD 'input' AS (B1:bag{T:tuple(val1:int,val2:int)},B2:bag{T:tuple(val1:int,val2:int)});
 * 
 * -- output:
 * -- ({(2,20),(3,30),(4,40),(1,10),(8,80)})
 * output = FOREACH input GENERATE SetUnion(B1,B2);
 * }
 * </pre>
 */
public class SetUnion extends SetOperationsBase
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    DataBag outputBag = bagFactory.newDistinctBag();

    try {
      for (int i=0; i < input.size(); i++) {
        Object o = input.get(i);
        if (!(o instanceof DataBag))
          throw new RuntimeException("parameters must be databags");

        DataBag inputBag = (DataBag) o;
        for (Tuple elem : inputBag) {
          outputBag.add(elem);
        }
      }

      return outputBag;
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }
}


