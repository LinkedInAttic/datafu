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
import java.util.Arrays;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes the {@link <a href="http://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval" target="_blank">Wilsonian binomial proportion confidence interval</a>}
 * <p>
 * Constructor requires the confidence interval (alpha) parameter, and the
 * parameters are the number of positive (success) outcomes and the total
 * number of observations. The UDF returns the (lower,upper) confidence
 * interval. 
 * <p>
 * Example:
 * <pre>
 * {@code
 * -- the Wilsonian binomial proportion confidence interval for scoring
 * %declare WILSON_ALPHA 0.10
 *
 * define WilsonBinConf      datafu.pig.stats.WilsonBinConf('$WILSON_ALPHA'); 
 *
 * bar = FOREACH foo GENERATE WilsonBinConf(successes, totals).lower as score;
 * quux = ORDER bar BY score DESC;
 * top = LIMIT quux 10;
 * }
 * </pre></p>
 */
public class WilsonBinConf extends SimpleEvalFunc<Tuple>
{
  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  private final double alpha;

  public WilsonBinConf(double alpha)
  {
    this.alpha = alpha;
  }

  public WilsonBinConf(String alpha)
  {
    this(Double.parseDouble(alpha));
  }

  public Tuple call(Number x, Number n) throws IOException
  {
    if (x == null || n == null)
      return null;
    return binconf(x.longValue(), n.longValue());
  }
  
  /**
   * @param x The number of positive (success) outcomes
   * @param n The number of observations
   * @return The (lower,upper) confidence interval
   */
  public Tuple binconf(Long x, Long n) throws IOException
  {
    NormalDistribution normalDist = new NormalDistributionImpl();

    if (x == null || n == null)
      return null;
    if (x < 0 || n < 0)
      throw new IllegalArgumentException("non-negative values expected");
    if (x > n)
      throw new IllegalArgumentException("invariant violation: number of successes > number of obs");
    if (n == 0)
      return tupleFactory.newTuple(Arrays.asList(Double.valueOf(0), Double.valueOf(0)));

    try {
      double zcrit = -1.0 * normalDist.inverseCumulativeProbability(alpha/2);
      double z2 = zcrit * zcrit;
      double p = x/(double)n;

      double a = p + z2/2/n;
      double b = zcrit * Math.sqrt((p * (1 - p) + z2/4/n)/n);
      double c = (1 + z2/n);

      double lower = (a - b) / c;
      double upper = (a + b) / c;

      // Add corrections for when x is very close to n.  This improves the estimates.
      // For more info on wilson binomial confidence interval, see paper:
      // L.D. Brown, T.T. Cai and A. DasGupta, Interval estimation for a binomial proportion (with discussion), 
      //   _Statistical Science,_*16*:101-133, 2001. 
      // http://www-stat.wharton.upenn.edu/~tcai/paper/Binomial-StatSci.pdf
      
      if (x == 1)
        lower = -Math.log(1 - alpha)/n;
      if (x == (n - 1))
        upper = 1 + Math.log(1 - alpha)/n;

      return tupleFactory.newTuple(Arrays.asList(lower, upper));
    }
    catch (MathException e) {
      throw new IOException("math error", e);
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      Schema innerSchema =  new  Schema(Arrays.asList(
              new Schema.FieldSchema("lower", DataType.DOUBLE),
              new Schema.FieldSchema("upper", DataType.DOUBLE)));

      return new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
    } catch(FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
