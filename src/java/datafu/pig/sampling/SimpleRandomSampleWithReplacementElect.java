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

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Select the candidate with the smallest score for each position from the candidates
 * proposed by {@link SimpleRandomSampleWithReplacementVote}.
 * 
 * @see SimpleRandomSampleWithReplacementVote
 * 
 * @author ximeng
 * 
 */
public class SimpleRandomSampleWithReplacementElect extends AlgebraicEvalFunc<DataBag>
{
  /**
   * Prefix for the output bag name.
   */
  public static final String OUTPUT_BAG_NAME_PREFIX = "SRSWR_ELECT";

  public static final TupleFactory tupleFactory = TupleFactory.getInstance();
  public static final BagFactory bagFactory = BagFactory.getInstance();

  static class CandidateComparator implements Comparator<Tuple>
  {
    private static CandidateComparator _instance = new CandidateComparator();

    public static CandidateComparator get()
    {
      return _instance;
    }

    private CandidateComparator()
    {
      // singleton
    }

    @Override
    public int compare(Tuple o1, Tuple o2)
    {
      try
      {
        // first by position
        int c1 = ((Integer) o1.get(0)).compareTo((Integer) o2.get(0));
        if (c1 != 0)
        {
          return c1;
        }
        else
        {
          // then by score
          return ((Double) o1.get(1)).compareTo((Double) o2.get(1));
        }
      }
      catch (ExecException e)
      {
        throw new RuntimeException("Error comparing tuples " + o1 + " and " + o2, e);
      }
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

  static public class Initial extends EvalFunc<Tuple>
  {
    @Override
    public Tuple exec(Tuple input) throws IOException
    {
      // output each input candidate
      return input;
    }
  }

  static public class Intermediate extends EvalFunc<Tuple>
  {
    @Override
    public Tuple exec(Tuple tuple) throws IOException
    {
      // sort candidates first by index, then by key
      DataBag candidates = bagFactory.newSortedBag(CandidateComparator.get());
      for (Tuple intermediateOutputTuple : (DataBag) tuple.get(0))
      {
        candidates.addAll((DataBag) intermediateOutputTuple.get(0));
      }

      DataBag outputBag = bagFactory.newDefaultBag();
      int i = -1;
      for (Tuple candidate : candidates)
      {
        int pos = (Integer) candidate.get(0);
        if (pos > i)
        {
          outputBag.add(candidate);
          i = pos;
        }
      }

      return tupleFactory.newTuple(outputBag);
    }

  }

  static public class Final extends EvalFunc<DataBag>
  {
    @Override
    public DataBag exec(Tuple tuple) throws IOException
    {
      DataBag candidates = bagFactory.newSortedBag(CandidateComparator.get());
      for (Tuple intermediateOutputTuple : (DataBag) tuple.get(0))
      {
        candidates.addAll((DataBag) intermediateOutputTuple.get(0));
      }

      DataBag outputBag = bagFactory.newDefaultBag();
      int i = -1;
      for (Tuple candidate : candidates)
      {
        int pos = (Integer) candidate.get(0);
        if (pos > i)
        {
          outputBag.add((Tuple) candidate.get(2));
          i = pos;
        }
      }
      return outputBag;
    }
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

      // the output is a bag of selected items
      Schema outputSchema =
          new Schema(new Schema.FieldSchema(super.getSchemaName(OUTPUT_BAG_NAME_PREFIX,
                                                                input),
                                            inputFieldSchema.schema.getField(0).schema.getField(2).schema,
                                            DataType.BAG));

      return outputSchema;
    }
    catch (FrontendException e)
    {
      throw new RuntimeException("Error deriving output schema.", e);
    }
  }
}
