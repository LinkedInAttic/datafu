/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package datafu.pig.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Computes approximate {@link <a href="http://en.wikipedia.org/wiki/Quantile" target="_blank">quantiles</a>} 
 * for a (not necessarily sorted) input bag, using the Munro-Paterson algorithm.
 * 
 * <p>
 * The algorithm is described here:
 * {@link <a href="www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf">http://www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf</a>}
 * </p>
 * 
 * <p>
 * The implementation is based on the one in Sawzall, available here:
 * {@link <a href="http://szl.googlecode.com/svn-history/r41/trunk/src/emitters/szlquantile.cc">szlquantile.cc</a>}
 * </p>
 * 
 * <p>
 * N.B., all the data is pushed to a single reducer per key, so make sure some partitioning is 
 * done (e.g., group by 'day') if the data is too large.  That is, this isn't distributed quantiles.
 * </p>
 * 
 * <p>
 * Note that unlike datafu's standard Quantile algorithm, the Munro-Paterson algorithm gives
 * <b>approximate</b> quantiles and does not require the input bag to be sorted.  Because it implements
 * accumulate, StreamingQuantile can be much more efficient than Quantile for large amounts of data which
 * do not fit in memory.  Quantile must spill to disk when the input data is too large to fit in memory, 
 * which will contribute to longer runtimes.
 * </p>
 * 
 * <p>The constructor takes a single integer argument that specifies the number of evenly-spaced 
 * quantiles to compute, e.g.,</p>
 * 
 * <ul>
 *   <li>StreamingQuantile('3') yields the min, the median, and the max
 *   <li>StreamingQuantile('5') yields the min, the 25th, 50th, 75th percentiles, and the max
 *   <li>StreamingQuantile('101') yields the min, the max, and all 99 percentiles.
 * </ul>
 * 
 * <p>Alternatively the constructor can take the explicit list of quantiles to compute, e.g.</p>
 *
 * <ul>
 *   <li>StreamingQuantile('0.0','0.5','1.0') yields the min, the median, and the max
 *   <li>StreamingQuantile('0.0','0.25','0.5','0.75','1.0') yields the min, the 25th, 50th, 75th percentiles, and the max
 * </ul>
 *
 * <p>The list of quantiles need not span the entire range from 0.0 to 1.0, nor do they need to be evenly spaced, e.g.</p>
 * 
 * <ul>
 *   <li>StreamingQuantile('0.5','0.90','0.95','0.99') yields the median, the 90th, 95th, and the 99th percentiles
 *   <li>StreamingQuantile('0.0013','0.0228','0.1587','0.5','0.8413','0.9772','0.9987') yields the 0.13th, 2.28th, 15.87th, 50th, 84.13th, 97.72nd, and 99.87th percentiles
 * </ul>
 *
 * <p>Be aware when specifying the list of quantiles in this way that more quantiles may be computed internally than are actually returned.
 * The GCD of the quantiles is found and this determines the number of evenly spaced quantiles to compute.  The requested quantiles
 * are then returned from this set.  For instance:</p>
 * 
 * <ul>
 *   <li>If the quantiles 0.2 and 0.6 are requested then the quantiles 0.0, 0.2, 0.4, 0.6, 0.8, and 1.0 are computed 
 *       because 0.2 is the GCD of 0.2, 0.6, and 1.0.</li>  
 *   <li>If 0.2 and 0.7 are requested then the quantiles 0.0, 0.1, 0.2, ... , 0.9, 1.0 are computed because 0.1 is the 
 *       GCD of 0.2, 0.7, and 1.0.</li>
 *   <li>If 0.999 is requested the quantiles 0.0, 0.001, 0.002, ... , 0.998, 0.999, 1.0 are computed because 0.001 is
 *       the GCD of 0.999 and 1.0.</li> 
 *  </p>  
 * </ul>
 * 
 * <p>The error on the approximation goes down as the number of buckets computed goes up.</p>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 *
 * define Quantile datafu.pig.stats.StreamingQuantile('5');

 * -- input: 9,10,2,3,5,8,1,4,6,7
 * input = LOAD 'input' AS (val:int);
 *
 * grouped = GROUP input ALL;
 *
 * -- produces: (1.0,3.0,5.0,8.0,10.0)
 * quantiles = FOREACH grouped generate Quantile(input);
 * }
 * </pre></p>
 *
 * @see StreamingMedian
 * @see Quantile
 */
public class StreamingQuantile extends AccumulatorEvalFunc<Tuple>
{
  private final int numQuantiles;
  private final QuantileEstimator estimator;
  private List<Double> quantiles;
 
 // For the output schema, label the quantiles 0, 1, 2, ... n
 // Otherwise label the quantiles based on the quantile value.
 // e.g. 50% quantile 0.5 will be labeled as 0_5
 private boolean ordinalOutputSchema;
  
  public StreamingQuantile(String... k)
  {
    if (k.length == 1 && Double.parseDouble(k[0]) > 1.0) 
    {
      this.ordinalOutputSchema = true;
      this.numQuantiles = Integer.parseInt(k[0]);
    }
    else
    {
      this.quantiles = QuantileUtil.getQuantilesFromParams(k);
      this.numQuantiles = getNumQuantiles(this.quantiles);
    }
    this.estimator = new QuantileEstimator(this.numQuantiles);
  }
  
  private static int getNumQuantiles(List<Double> quantiles)
  {
    quantiles = new ArrayList<Double>(quantiles);
    Collections.sort(quantiles);
    int start = 0;
    int end = quantiles.size()-1;
    while (quantiles.get(start) == 0.0) start++;
    while (quantiles.get(end) == 1.0) end--;
    double gcd = 1.0;
    for (int i=end; i>=start; i--)
    {
      gcd = gcd(gcd,quantiles.get(i));
    }
    int numQuantiles = (int)(1/gcd) + 1;
    return numQuantiles;
  }
  
  private static double gcd(double a, double b)
  {
    if (round(a) == 0.0)
    {
      throw new IllegalArgumentException("Quantiles are smaller than the allowed precision");
    }
    if (round(b) == 0.0)
    {
      throw new IllegalArgumentException("Quantiles are smaller than the allowed precision");
    }
    while (round(b) != 0.0)
    {
      double t = b;
      b = a % b;
      a = t;
    }
    return round(a);
  }
  
  private static double round(double d)
  {
    return Math.round(d*100000.0)/100000.0;
  }

  @Override
  public void accumulate(Tuple b) throws IOException
  {
    DataBag bag = (DataBag) b.get(0);
    if (bag == null || bag.size() == 0)
      return;

    for (Tuple t : bag) {
      Object o = t.get(0);
      if (!(o instanceof Number)) {
        throw new IllegalStateException("bag must have numerical values (and be non-null)");
      }
      estimator.add(((Number) o).doubleValue());
    }
  }

  @Override
  public void cleanup()
  {
    estimator.clear();
  }

  @Override
  public Tuple getValue()
  {
    Tuple t = TupleFactory.getInstance().newTuple(this.quantiles != null ? this.quantiles.size() : this.numQuantiles);
    try {
      if (this.quantiles == null)
      {
        int j = 0;
        for (double quantileValue : estimator.getQuantiles()) 
        {
          t.set(j, quantileValue);
          j++;
        }
      }
      else
      {
        HashMap<Double,Double> quantileValues = new HashMap<Double,Double>(this.quantiles.size());
        double quantileKey = 0.0;
        for (double quantileValue : estimator.getQuantiles()) {
          quantileValues.put(round(quantileKey), quantileValue);
          quantileKey += 1.0/(this.numQuantiles-1);
        }
        int j = 0;
        for (double d : this.quantiles)
        {
          Double quantileValue = quantileValues.get(round(d));
          t.set(j, quantileValue);
          j++;
        }
      }
    } catch (IOException e) {
      return null;
    }
    return t;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    if (ordinalOutputSchema)
    {
      for (int i = 0; i < this.numQuantiles; i++) 
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

  static class QuantileEstimator
  {
    private static final long MAX_TOT_ELEMS = 1024L * 1024L * 1024L * 1024L;

    private final List<List<Double>> buffer = new ArrayList<List<Double>>();
    private final int numQuantiles;
    private final int maxElementsPerBuffer;
    private int totalElements;
    private double min;
    private double max;
    
    public QuantileEstimator(int numQuantiles)
    {
      this.numQuantiles = numQuantiles;
      this.maxElementsPerBuffer = computeMaxElementsPerBuffer();
    }
    
    private int computeMaxElementsPerBuffer()
    {
      double epsilon = 1.0 / (numQuantiles - 1.0);
      int b = 2;
      while ((b - 2) * (0x1L << (b - 2)) + 0.5 <= epsilon * MAX_TOT_ELEMS) {
        ++b;
      }
      return (int) (MAX_TOT_ELEMS / (0x1L << (b - 1)));
    }
    
    private void ensureBuffer(int level)
    {
      while (buffer.size() < level + 1) {
        buffer.add(null);
      }
      if (buffer.get(level) == null) {
        buffer.set(level, new ArrayList<Double>());
      }
    }
    
    private void collapse(List<Double> a, List<Double> b, List<Double> out)
    {
      int indexA = 0, indexB = 0, count = 0;
      Double smaller = null;
      while (indexA < maxElementsPerBuffer || indexB < maxElementsPerBuffer) {
        if (indexA >= maxElementsPerBuffer ||
            (indexB < maxElementsPerBuffer && a.get(indexA) >= b.get(indexB))) {
          smaller = b.get(indexB++);
        } else {
          smaller = a.get(indexA++);
        }
        
        if (count++ % 2 == 0) {
          out.add(smaller);
        }
      }
      a.clear();
      b.clear();
    }
    
    private void recursiveCollapse(List<Double> buf, int level)
    {
      ensureBuffer(level + 1);
      
      List<Double> merged;
      if (buffer.get(level + 1).isEmpty()) {
        merged = buffer.get(level + 1);
      } else {
        merged = new ArrayList<Double>(maxElementsPerBuffer);
      }
      
      collapse(buffer.get(level), buf, merged);
      if (buffer.get(level + 1) != merged) {
        recursiveCollapse(merged, level + 1);
      }
    }
    
    public void add(double elem)
    {
      if (totalElements == 0 || elem < min) {
        min = elem;
      }
      if (totalElements == 0 || max < elem) {
        max = elem;
      }
      
      if (totalElements > 0 && totalElements % (2 * maxElementsPerBuffer) == 0) {
        Collections.sort(buffer.get(0));
        Collections.sort(buffer.get(1));
        recursiveCollapse(buffer.get(0), 1);
      }
      
      ensureBuffer(0);
      ensureBuffer(1);
      int index = buffer.get(0).size() < maxElementsPerBuffer ? 0 : 1;
      buffer.get(index).add(elem);
      totalElements++;
    }

    public void clear()
    {
      buffer.clear();
      totalElements = 0;
    }

    public List<Double> getQuantiles()
    {
      List<Double> quantiles = new ArrayList<Double>();
      quantiles.add(min);
      
      if (buffer.get(0) != null) {
        Collections.sort(buffer.get(0));
      }
      if (buffer.get(1) != null) {
        Collections.sort(buffer.get(1));
      }
      
      int[] index = new int[buffer.size()];
      long S = 0;
      for (int i = 1; i <= numQuantiles - 2; i++) {
        long targetS = (long) Math.ceil(i * (totalElements / (numQuantiles - 1.0)));
        
        while (true) {
          double smallest = max;
          int minBufferId = -1;
          for (int j = 0; j < buffer.size(); j++) {
            if (buffer.get(j) != null && index[j] < buffer.get(j).size()) {
              if (!(smallest < buffer.get(j).get(index[j]))) {
                smallest = buffer.get(j).get(index[j]);
                minBufferId = j;
              }
            }
          }
          
          long incrementS = minBufferId <= 1 ? 1L : (0x1L << (minBufferId - 1));
          if (S + incrementS >= targetS) {
            quantiles.add(smallest);
            break;
          } else {
            index[minBufferId]++;
            S += incrementS;
          }
        }
      }
      
      quantiles.add(max);
      return quantiles;
    }
  }
}
