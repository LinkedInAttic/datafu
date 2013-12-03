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
import java.util.List;
import java.util.Set;

import org.apache.commons.math.MathException;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

/**
 * Scalable simple random sampling with replacement (ScaSRSWR).
 * <p/>
 * This UDF together with {@link SimpleRandomSampleWithReplacementElect} implement a
 * scalable algorithm for simple random sampling with replacement (SRSWR), which is a
 * randomized algorithm with a failure rate less than {@value #FAILURE_RATE}.
 * <p/>
 * Let s be the desired sample size. To compute an SRSWR sample of size s, for each output
 * position in {0, 1, ..., s-1}, we want to select an item from the population uniformly
 * at random. This algorithm consists of two stages: vote and election. In the vote stage,
 * this UDF {@link SimpleRandomSampleWithReplacementVote} votes items, called candidates,
 * for each position. In the election stage, the paired UDF
 * {@link SimpleRandomSampleWithReplacementElect} elects one candidate for each position.
 * The algorithm succeeds if we have at least one candidate for each position.
 * <p/>
 * To use this UDF pair, user needs to provide: 1) the desired sample size, 2) a good
 * lower bound of the population size or the exact size. The input to the vote UDF
 * {@link SimpleRandomSampleWithReplacementVote} is a tuple that consists of a bag of
 * items, the desired sample size (int), and the population size (long) or a good lower
 * bound of it, where the latter two must be scalars. The output from the vote UDF is a
 * tuple that consists of position:int, score:double, and candidate. The input to the
 * elect UDF {@link SimpleRandomSampleWithReplacementElect} is a tuple that contains all
 * candidates voted by the vote UDF for some positions. The output from the elect UDF is a
 * bag of sampled items.
 * <p/>
 * For example, the following script generates a sample of size 100000 with replacement:
 * 
 * <pre>
 * DEFINE SRSWR_VOTE  datafu.pig.sampling.SimpleRandomSampleWithReplacementVote();
 * DEFINE SRSWR_ELECT datafu.pig.sampling.SimpleRandomSampleWithReplacementElect();
 * 
 * item       = LOAD 'input' AS (x:double); 
 * summary    = FOREACH (GROUP item ALL) GENERATE COUNT(item) AS count;
 * candidates = FOREACH item GENERATE FLATTEN(SRSWR_VOTE(TOBAG(x), 100000, summary.count));
 * sampled    = FOREACH (GROUP candidates BY position PARALLEL 10) GENERATE FLATTEN(SRSWR_ELECT(candidates));
 * </pre>
 * 
 * Because for election we only need to group candidates voted for the same position, this
 * algorithm can use many reducers to consume the candidates. See the "PARALLEL 10"
 * statement above. If the item to sample is the entire row, use TOBAG(TOTUPLE(*)).
 * <p/>
 * SRSWR is heavily used in bootstrapping. Bootstrapping can be done easily with this UDF
 * pair. For example, the following script generates 100 bootstrap samples, computes the
 * mean value for each sample, and then outputs the bootstrap estimates.
 * 
 * <pre>
 * summary    = FOREACH (GROUP item ALL) GENERATE AVG(item.x) AS mean, COUNT(item) AS count;
 * candidates = FOREACH item GENERATE FLATTEN(SRSWR_VOTE(TOBAG(x), summary.count*100, summary.count));
 * sampled    = FOREACH (GROUP candidates BY (position % 100) PARALLEL 10) GENERATE AVG(SRSWR_ELECT(candidates)) AS mean;
 * bootstrap  = FOREACH (GROUP sampled ALL) GENERATE summary.mean AS mean, sampled.mean AS bootstrapMeans;
 * </pre>
 * 
 * Another usage of this UDF pair is to generate random pairs or tuples without computing
 * the cross product, where each pair or tuple consist of items from different input
 * sources. Let s be the number of random tuples we want to generate. For each input
 * source, simply use the vote UDF to propose candidates, then join the candidates from
 * different sources by their positions and for each position use the elect UDF to select
 * one candidate from each source to form the pair or tuple for that position.
 * <p/>
 * The algorithm is a simple extension to the work
 * 
 * <pre>
 * X. Meng, Scalable Simple Random Sampling and Stratified Sampling, ICML 2013.
 * </pre>
 * 
 * Basically, for each output position, it performs a random sort on the population
 * (associates each item with a random score independently drawn from the uniform
 * distribution and then sorts items based on the scores), and picks the one that has the
 * smallest score. However, a probabilistic threshold is used to avoid sorting the entire
 * population. For example, if the population size is one billion and the random score
 * generated for an item is 0.9, very likely it won't become the smallest and hence we do
 * not need to propose it as a candidate.
 * <p/>
 * More precisely, let n be the population size, n1 be a good lower bound of n, s be the
 * sample size, delta be the failure rate, and q be the threshold. For each output
 * position the probability of all random scores being greater than q is (1-q)^n. Thus, if
 * we throw away items with associated scores greater than q, with probability at least 1
 * - s*(1-q)^n, we can still capture the item with the smallest score for each position.
 * Fix delta = s*(1-q)^n and solve for q, we get q = 1-exp(log(delta/s)/n), Note that
 * replacing n by n1 < n can only decrease the failure rate, though at the cost of
 * increased number of candidates. The expected number of candidates is (1 -
 * exp(log(delta/s)/n1)*s*n. When n1 equals n, this number is approximately
 * s*log(s/delta).
 * <p/>
 * Generating a random score for each (item, position) pair is very expensive and
 * unnecessary. For each item, the number of positions for which it gets voted follows a
 * binomial distribution B(s,q). We can simply draw a number from this distribution,
 * determine the positions by sampling without replacement, and then generate random
 * scores for those positions. This reduces the running time significantly.
 * <p/>
 * Since for each position we only need the candidate with the smallest score, we
 * implement a combiner to reduce the size of intermediate data in the elect UDF
 * {@link SimpleRandomSampleWithReplacementElect}.
 * 
 * @see SimpleRandomSampleWithReplacementElect
 * @see <a href="http://en.wikipedia.org/wiki/Bootstrapping_(statistics) target="_blank
 *      ">Boostrapping (Wikipedia)</a>
 * 
 * @author ximeng
 * 
 */
public class SimpleRandomSampleWithReplacementVote extends EvalFunc<DataBag>
{
  public static final String OUTPUT_BAG_NAME_PREFIX = "SRSWR_VOTE";
  public static final String CANDIDATE_FIELD_NAME = "candidate";
  public static final String POSITION_FIELD_NAME = "position";
  public static final String SCORE_FIELD_NAME = "score";
  public static final double FAILURE_RATE = 1e-4;

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final BagFactory bagFactory = BagFactory.getInstance();

  private RandomDataImpl _rdg = new RandomDataImpl();

  /**
   * Samples k integers from [0, n) without replacement efficiently.
   * 
   * If k is small, we can repeatedly draw integers from [0, n) until there are k distinct
   * values. For each trial, with probability at least (n-k)/n, we can draw a new value.
   * So the expected number of trials is smaller than (k*n)/(n-k), which is a very rough
   * bound. If k is large, we use the selection-rejection sampling algorithm. Basically,
   * we want the running time to be O(k).
   * 
   */
  private int[] sampleWithoutReplacement(int n, int k)
  {
    if (k == 0)
    {
      return new int[] {};
    }

    if (k < n / 3L)
    {
      Set<Integer> sample = Sets.newHashSetWithExpectedSize(k);

      // The expected number of iterations is less than 1.5*k
      while (sample.size() < k)
      {
        sample.add(_rdg.nextInt(0, n - 1));
      }

      return Ints.toArray(sample);
    }
    else
    {
      int[] sample = new int[k];

      int i = 0;
      for (int j = 0; j < n && i < k; ++j)
      {
        if (_rdg.nextUniform(0.0d, 1.0d) < 1.0d * (k - i) / (n - j))
        {
          sample[i] = j;
          i++;
        }
      }

      return sample;
    }
  }

  @Override
  public DataBag exec(Tuple tuple) throws IOException
  {
    if (tuple.size() != 3)
    {
      throw new IllegalArgumentException("The input arguments are: "
          + "a bag of items, the desired sample size (int), and the population size (long) or a good lower bound of it");
    }

    DataBag items = (DataBag) tuple.get(0);
    int sampleSize = ((Number) tuple.get(1)).intValue();
    long count = ((Number) tuple.get(2)).longValue();

    /*
     * The following threshold is to guarantee that each output position contains at least
     * one candidate with high probability.
     */
    double threshold = 1.0d - Math.exp(Math.log(FAILURE_RATE / sampleSize) / count);

    DataBag candidates = bagFactory.newDefaultBag();

    for (Tuple item : items)
    {
      // Should be able to support long sample size if nextBinomial supports long.
      int numOutputPositions;
      try
      {
        numOutputPositions = _rdg.nextBinomial(sampleSize, threshold);
      }
      catch (MathException e)
      {
        throw new RuntimeException("Failed to generate a binomial value with n = "
            + sampleSize + " and p = " + threshold, e);
      }
      for (int outputPosition : sampleWithoutReplacement(sampleSize, numOutputPositions))
      {
        Tuple candidate = tupleFactory.newTuple();
        candidate.append(outputPosition);
        candidate.append(_rdg.nextUniform(0.0d, 1.0d)); // generate a random score
        candidate.append(item);
        candidates.add(candidate);
      }
    }

    return candidates;
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

      List<Schema.FieldSchema> fieldSchemas = Lists.newArrayList();

      fieldSchemas.add(new Schema.FieldSchema(POSITION_FIELD_NAME, DataType.INTEGER));
      fieldSchemas.add(new Schema.FieldSchema(SCORE_FIELD_NAME, DataType.DOUBLE));
      fieldSchemas.add(new Schema.FieldSchema(CANDIDATE_FIELD_NAME,
                                              inputFieldSchema.schema.getField(0).schema));

      Schema outputSchema =
          new Schema(new Schema.FieldSchema(super.getSchemaName(OUTPUT_BAG_NAME_PREFIX,
                                                                input),
                                            new Schema(fieldSchemas),
                                            DataType.BAG));

      return outputSchema;
    }
    catch (FrontendException e)
    {
      throw new RuntimeException("Error deriving output schema.", e);
    }
  }
}
