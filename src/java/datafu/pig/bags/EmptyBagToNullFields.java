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
import java.util.Arrays;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.ContextualEvalFunc;

/**
 * For an empty bag, inserts a tuple having null values for all fields; 
 * otherwise, the input bag is returned unchanged.
 * 
 * <p>
 * This can be useful when performing FLATTEN on a bag from a COGROUP,
 * as FLATTEN on an empty bag produces no data.
 * </p>
 */
public class EmptyBagToNullFields extends ContextualEvalFunc<DataBag>
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
        // create a tuple with null values for all fields
        int tupleSize = (Integer)getInstanceProperties().get("tuplesize");
        return BagFactory.getInstance().newDefaultBag(Arrays.asList(TupleFactory.getInstance().newTuple(tupleSize)));
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
    try
    {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected only a single field as input");
      }
      
      if (input.getField(0).type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input, but found " + DataType.findTypeName(input.getField(0).type));
      }
      
      // get the size of the tuple within the bag
      int innerTupleSize = input.getField(0).schema.getField(0).schema.getFields().size();
      getInstanceProperties().put("tuplesize", innerTupleSize);
    }
    catch (FrontendException e)
    {
      throw new RuntimeException(e);
    }
    return input;
  }
}