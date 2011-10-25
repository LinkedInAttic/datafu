/*
 * Copyright 2010 LinkedIn, Inc
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes one or more k-th {@link <a href="http://en.wikipedia.org/wiki/Quantile" target="_blank">quantiles</a>} of a sorted bag, 
 * where 0 <= k <= 1.0.  Uses type R-2 estimation.
 *
 * The constructor argument takes the quantiles to compute. <b>The input bag must be sorted.</b> N.B., all the data
 * is pushed to a single reducer per key, so make sure some partitioning is done (e.g., group by 'day') if the data is too large.
 * That is, this isn't distributed quantiles.
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
 */
public class Quantile extends SimpleEvalFunc<Tuple> implements Accumulator<Tuple>
{
  List<Double> quantiles;
  Map<Long, Double> d = new HashMap<Long, Double>();
  long i;
  
  private static class Pair<T1,T2>
  {
    public T1 first;
    public T2 second;

    public Pair(T1 first, T2 second) {
      this.first = first;
      this.second = second;
    }
  }

  public Quantile(String... k)
  {
    quantiles = new ArrayList<Double>(k.length);
    for (String s : k) { 
      quantiles.add(Double.parseDouble(s));
    }
    cleanup();
  }

  private static Pair<Long, Long> getIndexes(double k, long N)
  {
    double h = N*k + 0.5;
    long i1 = Math.min(Math.max(1, (long)Math.ceil(h - 0.5)), N);
    long i2 = Math.min(Math.max(1, (long)Math.floor(h + 0.5)), N);

    return new Pair<Long, Long>(i1, i2);
  }

  @Override
  public void accumulate(Tuple b) throws IOException
  {
    DataBag bag = (DataBag) b.get(0);
    if (bag == null || bag.size() == 0)
      return;
        
    for (Tuple t : bag) {

      Object o = t.get(0);
      if (!(o instanceof Number))
        throw new IllegalStateException("bag must have numerical values (and be non-null)");
      d.put(i, ((Number) o).doubleValue());
      
      i++;
    }
  }

  @Override
  public Tuple getValue()
  {
    Tuple t = TupleFactory.getInstance().newTuple(this.quantiles.size());
    int j = 0;
    for (double k : this.quantiles) {
      Pair<Long, Long> p = getIndexes(k,i-1);
      double quantile = (d.get(p.first) + d.get(p.second)) / 2;
      try
      {
        t.set(j, quantile);
      }
      catch (ExecException e)
      {        
        e.printStackTrace();
        return null;
      }
      j++;
    }
    return t;
  }

  @Override
  public void cleanup()
  {    
    i = 1;   
    d.clear();
  }
  
  public Tuple call(DataBag bag) throws IOException
  {
    try
    {
      accumulate(TupleFactory.getInstance().newTuple(bag));
      return getValue();
    }
    finally
    {
      cleanup();
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    for (Double x : this.quantiles)
      tupleSchema.add(new Schema.FieldSchema("quantile_" + x.toString().replace(".", "_"), DataType.DOUBLE));
    return tupleSchema;
  }
}

