/*
 * Copyright 2013 LinkedIn, Inc
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
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Create a new bag by performing a weighted sampling without replacement
 * from the input bag. Optionally takes two additional arguments to specify
 * the index of the column to use as a scoring column (uses enumerated bag 
 * order if not specified) and a limit on the number of items to return.
 * n.b.
 * <ul>
 * <li>When no scoring column is specified, items from the top of the bag are
 * more likely to be chosen than items from the bottom.
 * <li>High scores are more likely to be chosen when using a scoring column.
 * </ul>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define WeightedSample com.linkedin.endorsements.pig.WeightedSample()
 * 
 * -- input:
 * -- ({(a,1),(b,2),(c,3),(d,4),(e,5)})
 * input = LOAD 'input' AS (A: bag{T: tuple(name:chararray,score:int)});
 * 
 * output1 = FOREACH input GENERATE WeightedSample(A);
 * -- output1:
 * -- no scoring column specified, so uses bag order
 * -- ({(a,1),(b,2),(e,5),(d,4),(c,3)}) -- example of random
 * 
 * -- sample using the second column (index 1) and keep only the top 3
 * -- scoring column is specified, so bias towards higher scores
 * -- only keep the first 3
 * output2 = FOREACH input GENERATE WeightedSample(A,1,3);
 * -- output2:
 * -- ({(e,5),(d,4),(b,2)})
 * }
 * </pre>
 */
public class WeightedSample extends EvalFunc<DataBag>
{
  BagFactory bagFactory = BagFactory.getInstance();

  public WeightedSample() {
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
    if (input.size() <= 1) {      
      // no scoring column specified, so use rank order
      for (int i = 0; i < scores.length; i++) {
        scores[i] = scores.length + 1 - i;
      }
    } else {
      tupleIndex = 0;
      int scoreIndex = ((Number)input.get(1)).intValue();
      for (Tuple tuple : samples) {
        double score = ((Number)tuple.get(scoreIndex)).doubleValue();
        score = Math.max(score, Double.MIN_NORMAL); // negative scores cause problems
        scores[tupleIndex] = score;
        tupleIndex++;
      }
    }
    
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
    Random rng = new Random();
    // the system property random seed is used to enable repeatable tests
    if (System.getProperties().containsKey("pigunit.randseed")) {
      long randSeed = Long.parseLong(System.getProperties().getProperty("pigunit.randseed"));
      rng = new Random(randSeed);
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
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG) {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               inputFieldSchema.schema, DataType.BAG));    
    } catch (FrontendException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
