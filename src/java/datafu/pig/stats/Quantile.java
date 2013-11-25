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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes {@link <a href="http://en.wikipedia.org/wiki/Quantile" target="_blank">quantiles</a>} 
 * for a <b>sorted</b> input bag, using type R-2 estimation.
 *
 * <p>
 * N.B., all the data is pushed to a single reducer per key, so make sure some partitioning is 
 * done (e.g., group by 'day') if the data is too large.  That is, this isn't distributed quantiles.
 * </p>
 * 
 * <p>
 * Note that unlike datafu's StreamingQuantile algorithm, this implementation gives
 * <b>exact</b> quantiles.  But, it requires that the input bag to be sorted.  Quantile must spill to 
 * disk when the input data is too large to fit in memory, which will contribute to longer runtimes. 
 * Because StreamingQuantile implements accumulate it can be much more efficient than Quantile for 
 * large input bags which do not fit well in memory.
 * </p>
 * 
 * <p>The constructor takes a single integer argument that specifies the number of evenly-spaced 
 * quantiles to compute, e.g.,</p>
 * 
 * <ul>
 *   <li>Quantile('3') yields the min, the median, and the max
 *   <li>Quantile('5') yields the min, the 25th, 50th, 75th percentiles, and the max
 *   <li>Quantile('101') yields the min, the max, and all 99 percentiles.
 * </ul>
 * 
 * <p>Alternatively the constructor can take the explicit list of quantiles to compute, e.g.</p>
 *
 * <ul>
 *   <li>Quantile('0.0','0.5','1.0') yields the min, the median, and the max
 *   <li>Quantile('0.0','0.25','0.5','0.75','1.0') yields the min, the 25th, 50th, 75th percentiles, and the max
 * </ul>
 *
 * <p>The list of quantiles need not span the entire range from 0.0 to 1.0, nor do they need to be evenly spaced, e.g.</p>
 * 
 * <ul>
 *   <li>Quantile('0.5','0.90','0.95','0.99') yields the median, the 90th, 95th, and the 99th percentiles
 *   <li>Quantile('0.0013','0.0228','0.1587','0.5','0.8413','0.9772','0.9987') yields the 0.13th, 2.28th, 15.87th, 50th, 84.13th, 97.72nd, and 99.87th percentiles
 * </ul>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 *
 * define Quantile datafu.pig.stats.Quantile('0.0','0.5','1.0');

 * -- input: 9,10,2,3,5,8,1,4,6,7
 * input = LOAD 'input' AS (val:int);
 *
 * grouped = GROUP input ALL;
 *
 * -- produces: (1,5.5,10)
 * quantiles = FOREACH grouped {
 *   sorted = ORDER input BY val;
 *   GENERATE Quantile(sorted);
 * }
 * }</pre></p>
 *
 * @see Median
 * @see StreamingQuantile
 */
public class Quantile extends SimpleEvalFunc<Tuple>
{
  List<Double> quantiles;

  private static class Pair<T1,T2>
  {
    public T1 first;
    public T2 second;

    public Pair(T1 first, T2 second) {
      this.first = first;
      this.second = second;
    }
  }
  
  // For the output schema, label the quantiles 0, 1, 2, ... n
  // Otherwise label the quantiles based on the quantile value.
  // e.g. 50% quantile 0.5 will be labeled as 0_5
  private boolean ordinalOutputSchema;

  public Quantile(String... k)
  {
    this.quantiles = QuantileUtil.getQuantilesFromParams(k);
    
    if (k.length == 1 && Double.parseDouble(k[0]) > 1.0) 
    {
      this.ordinalOutputSchema = true;
      this.quantiles = QuantileUtil.getQuantilesFromParams(k);
    }
    else
    {
      this.quantiles = QuantileUtil.getQuantilesFromParams(k);
    }
  }

  private static Pair<Long, Long> getIndexes(double k, long N)
  {
    double h = N*k + 0.5;
    long i1 = Math.min(Math.max(1, (long)Math.ceil(h - 0.5)), N);
    long i2 = Math.min(Math.max(1, (long)Math.floor(h + 0.5)), N);

    return new Pair<Long, Long>(i1, i2);
  }
  
  public Tuple call(DataBag bag) throws IOException
  {
    if (bag == null || bag.size() == 0)
      return null;

    Map<Long, Double> d = new HashMap<Long, Double>();
    long N = bag.size(), max_id = 1;
    
    for (double k : this.quantiles) {
      Pair<Long, Long> idx = getIndexes(k, N);

      d.put(idx.first, null);
      d.put(idx.second, null);
      max_id = Math.max(max_id, idx.second);
    }

    long i = 1;
    for (Tuple t : bag) {
      if (i > max_id)
        break;

      if (d.containsKey(i)) {
        Object o = t.get(0);
        if (!(o instanceof Number))
          throw new IllegalStateException("bag must have numerical values (and be non-null)");
        d.put(i, ((Number) o).doubleValue());
      }
      i++;
    }

    Tuple t = TupleFactory.getInstance().newTuple(this.quantiles.size());
    int j = 0;
    for (double k : this.quantiles) {
      Pair<Long, Long> p = getIndexes(k, N);
      double quantile = (d.get(p.first) + d.get(p.second)) / 2;
      t.set(j, quantile);
      j++;
    }
    return t;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    if (ordinalOutputSchema)
    {
      for (int i = 0; i < this.quantiles.size(); i++) 
      {
        tupleSchema.add(new Schema.FieldSchema("quantile_" + i, DataType.DOUBLE));
      }
    }
    else
    {
      for (Double x : this.quantiles)
        tupleSchema.add(new Schema.FieldSchema("quantile_" + x.toString().replace(".", "_"), DataType.DOUBLE));
    }

    try {
      return new Schema(new FieldSchema(null, tupleSchema, DataType.TUPLE));
    } catch(FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}

