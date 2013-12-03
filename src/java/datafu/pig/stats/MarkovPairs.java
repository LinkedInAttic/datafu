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
 
package datafu.pig.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


/**
 * Accepts a bag of tuples, with user supplied ordering, and generates pairs that can be used for
 * a Markov chain analysis. For example, if you had {(1), (4), (7)}, using the default lookahead of 1, you
 * get the pairs {
 *                ((1),(4)),
 *                ((4),(7))}
 * A lookahead factor tells the UDF how many steps in to the future to include. so, for a,b,c with a lookahead
 * of 2, a would be paired with both b and c.
 * The results are ordered are returned as ordered by the caller.
*/

public class MarkovPairs extends EvalFunc<DataBag>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  private static long lookahead_steps;

  private final int SPILL_THRESHOLD = 1000000;

  public MarkovPairs()
  {   
      MarkovPairs.lookahead_steps = 1;
  }
  
  public MarkovPairs(String lookahead_steps)
  {   
      MarkovPairs.lookahead_steps = Integer.valueOf(lookahead_steps);
  }

  /* start and end are inclusive. This forms transition pairs */
  private void generatePairs(ArrayList<Tuple> input, int start, int end, DataBag outputBag)
      throws ExecException
  {
    int count = 0;
    for (int i = start; (i + 1)<= end; i++) {
      Tuple elem1 = input.get(i);
      
      lookahead:
      for (int j = i+1; j <= i + lookahead_steps; j++)
      {
        if (j > end) break lookahead;
        Tuple elem2 = input.get(j);        
        if (count >= SPILL_THRESHOLD) {
          outputBag.spill();
          count = 0;
        }
        outputBag.add(tupleFactory.newTuple(Arrays.asList(elem1, elem2)));
        count ++;
      }
    }
  }

  @Override
  public DataBag exec(Tuple input)
      throws IOException
  {
    //things come in a tuple, in our case we have a bag (ordered views) passed. This is embedded in a length one tuple
    
    DataBag inputBag = (DataBag) input.get(0);         
    ArrayList<Tuple> inputData = new ArrayList<Tuple>();

    for (Tuple tuple : inputBag) {
      inputData.add(tuple);
    }

    int inputSize = inputData.size();

    try {
      DataBag outputBag = bagFactory.newDefaultBag();

      int startPos = 0;

      int stopPos = inputSize - 1;
      generatePairs(inputData, startPos, stopPos, outputBag);

      // set startPos for the next bucket
      startPos = stopPos + 1;
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
      Schema tupleSchema = new Schema();
                 
      FieldSchema fieldSchema = input.getField(0);
      
      if (fieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException(String.format("Expected input schema to be BAG, but instead found %s",
                                                 DataType.findTypeName(fieldSchema.type)));
      }
      
      FieldSchema fieldSchema2 = fieldSchema.schema.getField(0);
      
      tupleSchema.add(new Schema.FieldSchema("elem1", fieldSchema2.schema));
      tupleSchema.add(new Schema.FieldSchema("elem2", fieldSchema2.schema));
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               tupleSchema,
                                               DataType.BAG));
    }
    catch (Exception e) {
      return null;
    }
  }
  
}
