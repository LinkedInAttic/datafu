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

package datafu.pig.stats.entropy.stream;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.stats.entropy.EntropyUtil;


/**
 * Calculate entropy of a given bag of raw data samples according to entropy's definition in 
 * {@link <a href="http://en.wikipedia.org/wiki/Entropy_%28information_theory%29" target="_blank">wiki</a>} 
 * <p>
 * This UDF extends * {@link org.apache.pig.AccumulatorEvalFunc} and calculates
 * entropy in a streaming way. 
 * </p>
 * 
 * <p>
 * Its constructor has 2 arguments, the first argument specifies
 * the type of entropy estimator algorithm to apply and the second argument specifies
 * the base of logarithm 
 * </p>
 * 
 * <p>
 * The 1st argument, the type of entropy estimator algorithm we currently support, includes:
 * <ul>
 *     <li>empirical (empirical entropy estimator)
 *     <li>chaosh (Chao-Shen entropy estimator) 
 * </ul>
 * The default estimation algorithm we support is empirical.
 * </p> 
 * 
 * <p>
 * The 2nd argument, the logarithm base we currently support, includes:
 * <ul>
 *     <li>log (use Euler's number as the logarithm base)
 *     <li>log2 (use 2 as the logarithm base)
 *     <li>log10 (use 10 as the logarithm base) 
 * </ul>
 * The default logarithm base we support is log
 * </p> 
 * 
 * <p>
 * Note:
 * <ul>
 *     <li>input bag to the UDF must be sorted. 
 *     <li>Entropy value is returned as double type.
 * </ul>
 * </p>
 *
 * <p>
 * How to use: This UDF is suitable to calculate entropy in a nested FOREACH after a GROUP BY,
 * where we sort the bag per group key and use the sorted bag as the input to this UDF, a scenario
 * we would like to calculate entropy per group.
 * 
 * Example:
 * <pre>
 * 
 * {@code
 * 
 * --calculate empirical entropy with Euler's number as the logarithm base
 * define Entropy datafu.pig.stats.entropy.stream.StreamingEntropy();
 *
 * input = LOAD 'input' AS (grp: chararray, val: double);
 *
 * -- calculate the input samples' entropy in each group
 * input_group_g = GROUP input BY grp;
 * entropy_group = FOREACH input_group_g {
 *   input_val = input.val;
 *   input_ordered = ORDER input_val BY $0;
 *   GENERATE FLATTEN(group) AS group, Entropy(input_ordered) AS entropy; 
 * }
 * 
 * }
 *
 * </pre>
 * 
 * </p>
 */
@Nondeterministic
public class StreamingEntropy extends AccumulatorEvalFunc<Double>
{ 
  //last visited tuple
  private Tuple x;
  
  //number of occurrence of last visited tuple
  private long cx;
  
  //comparison result between the present tuple and the last visited tuple
  private int lastCmp;
  
  //entropy estimator that accumulates sample's occurrence frequency to
  //calculates the actual entropy
  private EntropyEstimator estimator;
  
  public StreamingEntropy() throws ExecException
  {
    this(EntropyEstimator.EMPIRICAL_ESTIMATOR);
  }
  
  public StreamingEntropy(String type) throws ExecException 
  {
    this(type, EntropyUtil.LOG);
  }

  public StreamingEntropy(String type, String base) throws ExecException
  {
    try {
        this.estimator = EntropyEstimator.createEstimator(type, base);
    } catch (IllegalArgumentException ex) {
        throw new ExecException(
                String.format("Fail to initialize StreamingEntropy with entropy estimator of type (%s), base: (%s), exception: (%s)",
                       type, base, ex) 
              ); 
    }
    cleanup();
  }

  /*
   * Accumulate occurrence frequency of each tuple as we stream through the input bag
   */
  @Override
  public void accumulate(Tuple input) throws IOException
  {
    for (Tuple t : (DataBag) input.get(0)) {

      if (this.x != null)
      {
          int cmp = t.compareTo(this.x);
          
          //check if the comparison result is different from previous compare result
          if ((cmp < 0 && this.lastCmp > 0)
              || (cmp > 0 && this.lastCmp < 0)) {
              throw new ExecException("Out of order! previous tuple: " + this.x + ", present tuple: " + t
                                      + ", comparsion: " + cmp + ", previous comparsion: " + this.lastCmp);
          }

          if (cmp != 0) {
             //different tuple
             this.estimator.accumulate(this.cx);
             this.cx = 0;
             this.lastCmp = cmp;
          } 
      }

      //set tuple t as the next tuple for comparison
      this.x = t;

      //accumulate cx
      this.cx++;
    }
  }

  @Override
  public Double getValue()
  {
    //do not miss the last tuple
    try {
        this.estimator.accumulate(this.cx);
    } catch (ExecException ex) {
        throw new RuntimeException("Error while accumulating sample frequency: " + ex);
    }

    return this.estimator.getEntropy();
  }

  @Override
  public void cleanup()
  {
    this.x = null;
    this.cx = 0;
    this.lastCmp = 0;
    this.estimator.reset();
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
      try {
          Schema.FieldSchema inputFieldSchema = input.getField(0);

          if (inputFieldSchema.type != DataType.BAG)
          {
            throw new RuntimeException("Expected a BAG as input");
          }
          
          Schema inputBagSchema = inputFieldSchema.schema;
          
          if (inputBagSchema.getField(0).type != DataType.TUPLE)
          {
            throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                     DataType.findTypeName(inputBagSchema.getField(0).type)));
          }
          
          return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                                 .getName()
                                                                 .toLowerCase(), input),
                                               DataType.DOUBLE));
        } catch (FrontendException e) {
          throw new RuntimeException(e);
        }
   }
}
