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

package datafu.pig.sampling;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Performs weighted bernoulli sampling on a bag. 
 * 
 * <p>
 * Create a new bag by performing a weighted sampling without replacement
 * from the input bag. Sampling is biased according to a weight that
 * is part of the inner tuples in the bag.  That is, tuples with relatively 
 * high weights are more likely to be chosen over tuples with low weights. 
 * Optionally, a limit on the number of items to return may be specified.
 * </p>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define WeightedSample datafu.pig.sampling.WeightedSample()
 * 
 * -- input:
 * -- ({(a, 100),(b, 1),(c, 5),(d, 2)})
 * input = LOAD 'input' AS (A: bag{T: tuple(name:chararray,score:int)});
 * 
 * output1 = FOREACH input GENERATE WeightedSample(A,1);
 * -- output1:
 * -- uses the field indexed by 1 as a score
 * -- ({(a,100),(c,5),(b,1),(d,2)}) -- example of random
 * 
 * -- sample using the second column (index 1) and keep only the top 3
 * output2 = FOREACH input GENERATE WeightedSample(A,1,3);
 * -- output2:
 * -- ({(a,100),(c,5),(b,1)})
 * }
 * </pre>
 */
@Nondeterministic
public class WeightedSample extends EvalFunc<DataBag>
{
  BagFactory bagFactory = BagFactory.getInstance();
  Long seed = null;

  public WeightedSample() {
  }
  
  public WeightedSample(String seed) {
    this.seed = Long.parseLong(seed);
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {   
    DataBag output = bagFactory.newDefaultBag();

    DataBag samples = (DataBag) input.get(0);
    if (samples == null || samples.size() == 0) {
      return output; // if we are given null we will return an empty bag
    }
    int numSamples = (int) samples.size();
    if (numSamples == 1) return samples;
       
    Tuple[] tuples = new Tuple[numSamples];
    int tupleIndex = 0;
    for (Tuple tuple : samples) {
      tuples[tupleIndex] = tuple;
      tupleIndex++;
    }

    double[] scores = new double[numSamples];
    int scoreIndex = ((Number)input.get(1)).intValue();
    tupleIndex = 0;
    for (Tuple tuple : samples) {
      double score = ((Number)tuple.get(scoreIndex)).doubleValue();
      score = Math.max(score, Double.MIN_NORMAL); // negative scores cause problems
      scores[tupleIndex] = score;
      tupleIndex++;
    }
    
    // accept any type of number for sample size, but convert to int
    int limitSamples = numSamples;
    if (input.size() == 3) {
      // sample limit included
      limitSamples = Math.min(((Number)input.get(2)).intValue(), numSamples);      
    }

    /*
     * Here's how the algorithm works:
     * 
     * 1. Create a cumulative distribution of the scores 2. Draw a random number 3. Find
     * the interval in which the drawn number falls into 4. Select the element
     * encompassing that interval 5. Remove the selected element from consideration 6.
     * Repeat 1-5 k times
     * 
     * However, rather than removing the element (#5), which is expensive for an array,
     * this function performs some extra bookkeeping by replacing the selected element
     * with an element from the front of the array and truncating the front. This
     * complicates matters as the element positions have changed, so another mapping for
     * positions is needed.
     * 
     * This is an O(k*n) algorithm, where k is the number of elements to sample and n is
     * the number of scores.
     */    
    Random rng = null;    
    if (seed == null) {
      rng = new Random();
    } else {
      rng = new Random(seed);
    }
    
    for (int k = 0; k < limitSamples; k++) {
      double val = rng.nextDouble();
      int idx = find_cumsum_interval(scores, val, k, numSamples);
      if (idx == numSamples)
        idx = rng.nextInt(numSamples - k) + k;

      output.add(tuples[idx]);

      scores[idx] = scores[k];
      tuples[idx] = tuples[k];
    }

    return output;
  }

  public int find_cumsum_interval(double[] scores, double val, int begin, int end) {
    double sum = 0.0;
    double cumsum = 0.0;
    for (int i = begin; i < end; i++) {
      sum += scores[i];
    }

    for (int i = begin; i < end; i++) {
      cumsum += scores[i];
      if ((cumsum / sum) > val)
        return i;
    }
    return end;
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      if (!(input.size() == 2 || input.size() == 3))
      {
        throw new RuntimeException("Expected input to have two or three fields");
      }
      
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG) {
        throw new RuntimeException("Expected a BAG as first input, got: "+inputFieldSchema.type);
      }
      
      if (input.getField(1).type != DataType.INTEGER) {
        throw new RuntimeException("Expected an INT as second input, got: "+input.getField(1).type);
      }      
      
      if (input.size() == 3 && !(input.getField(2).type == DataType.INTEGER || input.getField(2).type == DataType.LONG)) {
        throw new RuntimeException("Expected an INT or LONG as second input, got: "+input.getField(2).type);
      }
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               inputFieldSchema.schema, DataType.BAG));    
    } catch (FrontendException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
