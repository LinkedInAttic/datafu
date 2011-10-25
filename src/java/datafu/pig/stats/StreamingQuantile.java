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
import java.util.Collections;
import java.util.List;

import org.apache.pig.Accumulator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes approximate quantiles for a (not necessarily sorted) input bag, using the
 * Munro-Paterson algorithm described here:
 * 
 * <a href="www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf">http://www.cs.ucsb.edu/~suri/cs290/MunroPat.pdf</a>
 * 
 * <br>The implementation is based on the one in Sawzall, available here:
 * <a href="http://szl.googlecode.com/svn-history/r41/trunk/src/emitters/szlquantile.cc">szlquantile.cc</a>
 * 
 * <p>
 * Note that unlike datafu's standard Quantile algorithm, the Munro-Paterson algorithm gives
 * <b>approximate</b> quantiles and does not require the input bag to be sorted. The constructor takes
 * a single integer argument that specifies the number of evenly-spaced quantiles to compute, e.g.,
 * 
 * <ul>
 *   <li>StreamingQuantile('3') yields the min, the median, and the max
 *   <li>StreamingQuantile('5') yields the min, the 25th, 50th, 75th percentiles, and the max
 *   <li>StreamingQuantile('101') yields the min, the max, and all 99 percentiles.
 * </ul>
 * 
 * <br>The error on the approximation goes down as the number of buckets computed goes up.
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
 * </pre></p>
 *
 */
public class StreamingQuantile extends SimpleEvalFunc<Tuple> implements Accumulator<Tuple> {

  private final int numQuantiles;
  private final QuantileEstimator estimator;
 
  public StreamingQuantile(String numQuantiles)
  {
    this.numQuantiles = Integer.valueOf(numQuantiles);
    this.estimator = new QuantileEstimator(this.numQuantiles);
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
    Tuple t = TupleFactory.getInstance().newTuple(numQuantiles);
    int j = 0;
    try {
      for (double quantile : estimator.getQuantiles()) {
        t.set(j, quantile);
        j++;
      }
    } catch (IOException e) {
      return null;
    }
    return t;
  }

  public Tuple call(DataBag b) throws IOException
  {
    accumulate(TupleFactory.getInstance().newTuple(b));
    Tuple ret = getValue();
    cleanup();
    return ret;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    for (int i = 0; i < numQuantiles; i++) {
      tupleSchema.add(new Schema.FieldSchema("quantile_" + i, DataType.DOUBLE));
    }
    return tupleSchema;
  }

  static class QuantileEstimator
  {
    private static final long MAX_TOT_ELEMS = 1024L * 1024L * 1024L * 1024L;

    private final List<List<Double>> buffer = Lists.newArrayList();
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
        buffer.set(level, Lists.<Double>newArrayList());
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
        merged = Lists.newArrayListWithCapacity(maxElementsPerBuffer);
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
      List<Double> quantiles = Lists.newArrayList();
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
