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
import java.util.Comparator;

import org.apache.commons.math.random.RandomDataImpl;
import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Scalable simple random sampling.
 * <p/>
 * This UDF implements a scalable simple random sampling algorithm described in
 * 
 * <pre>
 * X. Meng, Scalable Simple Random Sampling and Stratified Sampling, ICML 2013.
 * </pre>
 * 
 * It takes a sampling probability p as input and outputs a simple random sample of size
 * exactly ceil(p*n) with probability at least 99.99%, where $n$ is the size of the
 * population. This UDF is very useful for stratified sampling. For example,
 * 
 * <pre>
 * DEFINE SRS datafu.pig.sampling.SimpleRandomSample('0.01');
 * examples = LOAD ...
 * grouped = GROUP examples BY label;
 * sampled = FOREACH grouped GENERATE FLATTEN(SRS(examples));
 * STORE sampled ...
 * </pre>
 * 
 * We note that, in a Java Hadoop job, we can output pre-selected records directly using
 * MultipleOutputs. However, this feature is not available in a Pig UDF. So we still let
 * pre-selected records go through the sort phase. However, as long as the sample size is
 * not huge, this should not be a big problem.
 * 
 * @author ximeng
 * 
 */
public class SimpleRandomSample extends AlgebraicEvalFunc<DataBag>
{
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = BagFactory.getInstance();

  public SimpleRandomSample()
  {
  }

  public SimpleRandomSample(String samplingProbability)
  {
    Double p = Double.parseDouble(samplingProbability);

    if (p < 0.0 || p > 1.0)
    {
      throw new IllegalArgumentException("Sampling probability must be inside [0, 1].");
    }
  }

  @Override
  public String getInitial()
  {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed()
  {
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal()
  {
    return Final.class.getName();
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try
    {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }

      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                                 .getName()
                                                                 .toLowerCase(), input),
                                               inputFieldSchema.schema,
                                               DataType.BAG));
    }
    catch (FrontendException e)
    {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  static public class Initial extends EvalFunc<Tuple>
  {
    private double _samplingProbability;
    private RandomDataImpl _rdg = new RandomDataImpl();

    public Initial()
    {
    }

    public Initial(String samplingProbability)
    {
      _samplingProbability = Double.parseDouble(samplingProbability);
    }

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
      Tuple output = tupleFactory.newTuple();
      DataBag selected = bagFactory.newDefaultBag();
      DataBag waiting = bagFactory.newSortedBag(new ScoredTupleComparator());

      DataBag items = (DataBag) input.get(0);

      if (items != null)
      {
        long n = items.size();

        double q1 = getQ1(n, _samplingProbability);
        double q2 = getQ2(n, _samplingProbability);

        for (Tuple item : items)
        {
          double key = _rdg.nextUniform(0.0d, 1.0d);

          if (key < q1)
          {
            selected.add(item);
          }
          else if (key < q2)
          {
            waiting.add(new ScoredTuple(key, item).getIntermediateTuple(tupleFactory));
          }
        }

        output.append(n);
        output.append(selected);
        output.append(waiting);
      }

      return output;
    }
  }

  public static class Intermediate extends EvalFunc<Tuple>
  {
    public Intermediate()
    {
    }

    public Intermediate(String samplingProbability)
    {
      _samplingProbability = Double.parseDouble(samplingProbability);
    }

    private double _samplingProbability;

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
      DataBag bag = (DataBag) input.get(0);
      DataBag selected = bagFactory.newDefaultBag();
      DataBag aggWaiting = bagFactory.newSortedBag(new ScoredTupleComparator());
      DataBag waiting = bagFactory.newSortedBag(new ScoredTupleComparator());
      Tuple output = tupleFactory.newTuple();

      long n = 0L;

      for (Tuple innerTuple : bag)
      {
        n += (Long) innerTuple.get(0);

        selected.addAll((DataBag) innerTuple.get(1));

        double q1 = getQ1(n, _samplingProbability);
        double q2 = getQ2(n, _samplingProbability);

        for (Tuple t : (DataBag) innerTuple.get(2))
        {
          ScoredTuple scored = ScoredTuple.fromIntermediateTuple(t);

          if (scored.getScore() < q1)
          {
            selected.add(scored.getTuple());
          }
          else if (scored.getScore() < q2)
          {
            aggWaiting.add(t);
          }
          else
          {
            break;
          }
        }
      }

      double q1 = getQ1(n, _samplingProbability);
      double q2 = getQ2(n, _samplingProbability);

      for (Tuple t : aggWaiting)
      {
        ScoredTuple scored = ScoredTuple.fromIntermediateTuple(t);

        if (scored.getScore() < q1)
        {
          selected.add(scored.getTuple());
        }
        else if (scored.getScore() < q2)
        {
          waiting.add(t);
        }
        else
        {
          break;
        }
      }

      output.append(n);
      output.append(selected);
      output.append(waiting);

      System.err.println("Read " + n + " items, selected " + selected.size()
          + ", and wait-listed " + aggWaiting.size() + ".");

      return output;
    }
  }

  static public class Final extends EvalFunc<DataBag>
  {
    private double _samplingProbability;

    public Final()
    {
    }

    public Final(String samplingProbability)
    {
      _samplingProbability = Double.parseDouble(samplingProbability);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException
    {
      DataBag bag = (DataBag) input.get(0);
      long n = 0L;
      DataBag selected = bagFactory.newDefaultBag();
      DataBag waiting = bagFactory.newSortedBag(new ScoredTupleComparator());

      for (Tuple innerTuple : bag)
      {
        n += (Long) innerTuple.get(0);
        selected.addAll((DataBag) innerTuple.get(1));
        waiting.addAll((DataBag) innerTuple.get(2));
      }

      long sampleSize = (long) Math.ceil(_samplingProbability * n);
      long nNeeded = sampleSize - selected.size();

      for (Tuple scored : waiting)
      {
        if (nNeeded <= 0)
        {
          break;
        }
        selected.add(ScoredTuple.fromIntermediateTuple(scored).getTuple());
        nNeeded--;
      }

      return selected;
    }
  }

  private static class ScoredTupleComparator implements Comparator<Tuple>
  {

    @Override
    public int compare(Tuple o1, Tuple o2)
    {
      try
      {
        ScoredTuple t1 = ScoredTuple.fromIntermediateTuple(o1);
        ScoredTuple t2 = ScoredTuple.fromIntermediateTuple(o2);
        return t1.getScore().compareTo(t2.getScore());
      }
      catch (Throwable e)
      {
        throw new RuntimeException("Cannot compare " + o1 + " and " + o2 + ".", e);
      }
    }
  }

  private static double getQ1(long n, double p)
  {
    double t1 = 20.0 / (3.0 * n);
    double q1 = p + t1 - Math.sqrt(t1 * t1 + 3.0 * t1 * p);
    return q1;
  }

  private static double getQ2(long n, double p)
  {
    double t2 = 10.0 / n;
    double q2 = p + t2 + Math.sqrt(t2 * t2 + 2.0 * t2 * p);
    return q2;
  }
}
