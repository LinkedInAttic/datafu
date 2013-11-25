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

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Appends a tuple to a bag.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define AppendToBag datafu.pig.bags.AppendToBag();
 * 
 * -- input:
 * -- ({(1),(2),(3)},(4))
 * -- ({(10),(20),(30),(40),(50)},(60))
 * input = LOAD 'input' AS (B: bag{T: tuple(v:INT)}, T: tuple(v:INT));

 * -- output:
 * -- ({(1),(2),(3),(4)})
 * -- ({(10),(20),(30),(40),(50),(60)})
 * output = FOREACH input GENERATE AppendToBag(B,T) as B;
 * }
 * </pre>
 */
public class AppendToBag extends SimpleEvalFunc<DataBag>
{
  public DataBag call(DataBag inputBag, Tuple t) throws IOException
  {
    inputBag.add(t);
    return inputBag;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
              input.getField(0).schema, DataType.BAG));
    }
    catch (FrontendException e) {
      return null;
    }
  }
}
