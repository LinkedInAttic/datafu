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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Splits a bag of tuples into a bag of bags, where the inner bags collectively contain
 * the tuples from the original bag.  This can be used to split a bag into a set of smaller bags.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define BagSplit datafu.pig.bags.BagSplit();
 * 
 * -- input:
 * -- ({(1),(2),(3),(4),(5),(6),(7)})
 * -- ({(1),(2),(3),(4),(5)})
 * -- ({(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11)})
 * input = LOAD 'input' AS (B:bag{T:tuple(val1:INT,val2:INT)});
 * 
 * -- ouput:
 * -- ({{(1),(2),(3),(4),(5)},{(6),(7)}})
 * -- ({{(1),(2),(3),(4),(5)},{(6),(7),(8),(9),(10)},{(11)}})
 * output = FOREACH input GENERATE BagSplit(5,B);
 * }
 * </pre>
 */
public class BagSplit extends EvalFunc<DataBag>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    
  private final boolean appendBagNum;
  
  public BagSplit()
  {    
    this.appendBagNum = false;
  }
  
  public BagSplit(String appendBagNum)
  {
    this.appendBagNum = Boolean.parseBoolean(appendBagNum);
  }
  
  @Override
  public DataBag exec(Tuple arg0) throws IOException
  { 
    DataBag outputBag = bagFactory.newDefaultBag();
    
    Integer maxSize = (Integer)arg0.get(0);
    
    Object o = arg0.get(1);
    if (!(o instanceof DataBag))
      throw new RuntimeException("parameter must be a databag");
    
    DataBag inputBag = (DataBag)o;
    
    DataBag currentBag = null;
    
    int count = 0;
    int numBags = 0;
    for (Tuple tuple : inputBag)
    {
      if (currentBag == null)
      {
        currentBag = bagFactory.newDefaultBag();
      }
      
      currentBag.add(tuple);
      count++;
      
      if (count >= maxSize)
      {
        Tuple newTuple = tupleFactory.newTuple();
        newTuple.append(currentBag);
        
        if (this.appendBagNum)
        {
          newTuple.append(numBags);
        }
        
        numBags++;
        
        outputBag.add(newTuple);
        
        count = 0;
        currentBag = null;
      }
    }
    
    if (currentBag != null)
    {
      Tuple newTuple = tupleFactory.newTuple();
      newTuple.append(currentBag);
      if (this.appendBagNum)
      {
        newTuple.append(numBags);
      }
      outputBag.add(newTuple);
    }
        
    return outputBag;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.getField(0).type != DataType.INTEGER)
      {
        throw new RuntimeException("Expected first argument to be an INTEGER");
      }
      
      if (input.getField(1).type != DataType.BAG)
      {
        throw new RuntimeException("Expected second argument to be a BAG");
      }
      
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("data", input.getField(1).schema.clone(), DataType.BAG));
      
      if (this.appendBagNum)
      {
        tupleSchema.add(new Schema.FieldSchema("index", DataType.INTEGER));
      }
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               tupleSchema, DataType.BAG));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
