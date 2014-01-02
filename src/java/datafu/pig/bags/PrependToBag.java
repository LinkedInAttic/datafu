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

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Prepends a tuple to a bag. 
 * 
 * <p>N.B. this copies the entire input bag, so don't use it for large bags.</p>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define PrependToBag datafu.pig.bags.PrependToBag();
 * 
 * -- input:
 * -- ({(1),(2),(3)},(4))
 * -- ({(10),(20),(30),(40),(50)},(60))
 * input = LOAD 'input' AS (B: bag{T: tuple(v:INT)}, T: tuple(v:INT));

 * -- output:
 * -- ({(4),(1),(2),(3)})
 * -- ({(60),(10),(20),(30),(40),(50)})
 * output = FOREACH input GENERATE PrependToBag(B,T) as B;
 * }
 * </pre>
 * </p>
 */
public class PrependToBag extends SimpleEvalFunc<DataBag>
{
  public DataBag call(DataBag inputBag, Tuple t) throws IOException
  {
    DataBag outputBag = BagFactory.getInstance().newDefaultBag();
    outputBag.add(t);
    for (Tuple x : inputBag)
      outputBag.add(x);
    return outputBag;
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
