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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Returns null if the input is an empty bag; otherwise,
 * returns the input bag unchanged.
 */
public class EmptyBagToNull extends EvalFunc<DataBag>
{
  @Override
  public DataBag exec(Tuple tuple) throws IOException
  {
    if (tuple.size() == 0 || tuple.get(0) == null)
      return null;
    Object o = tuple.get(0);
    if (o instanceof DataBag)
    {
      DataBag bag = (DataBag)o;
      if (bag.size() == 0)
      {
        return null;
      }
      else
      {
        return bag;
      }
    }
    else
      throw new IllegalArgumentException("expected a null or a bag");
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return input;
  }
}