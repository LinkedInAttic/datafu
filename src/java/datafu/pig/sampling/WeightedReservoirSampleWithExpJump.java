/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *           http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package datafu.pig.sampling;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.backend.executionengine.ExecException;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Performs a weighted random sample using an in-memory reservoir to produce
 * a weighted random sample of a given size based on the A-ExpJ algorithm described in 
 * {@link <a href="http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf" target="_blank">paper</a>}. 
 * </p>
 * <p>
 * This UDF inherits from {@link WeightedReservoirSample}.
 * Similar to WeightedReservoirSample, items with larger weight has higher probability to be selected
 * in the final sample set. Since it uses exponential jumps and generates key per jump interval instead of per item,
 * it has higher running performance than WeightedReservoirSample when processing large volume of data.
 * </p>
 * <p>
 * Its constructor takes 2 arguments. For argument definition, please refer to {@link WeightedReservoirSample}.
 * </p>
 * <p>
 * Example:
 * <pre>
 * {@code
 * define WeightedSample datafu.pig.sampling.WeightedReservoirSampleWithExpJump('1','1');
 * input = LOAD 'input' AS (v1:chararray, v2:INT);
 * input_g = GROUP input ALL;
 * sampled = FOREACH input_g GENERATE WeightedSample(input);
 * }
 * </pre>
 * </p>
 * @author wjian
 */

@Nondeterministic
public class WeightedReservoirSampleWithExpJump extends WeightedReservoirSample 
{
    //value smaller than LOW_EPISLON will cause Math.log() equals to -Infinity
    private final static double LOW_EPISLON = 1e-323;
    
    //value larger than HIGH_EPISLON will cause Math.log() equals to 0
    private final static double HIGH_EPISLON = (1.0 - 1e-16);
    
    private WeightedReservoirSampleWithExpJump(int numSamples, int weightIdx)
    {
        super(new InverseWeightJumpSampleReservoir(numSamples, weightIdx));
        super.weightIdx = weightIdx;
    }
    
    public WeightedReservoirSampleWithExpJump(String strNumSamples, String strWeightIdx) 
    {
        this(Integer.parseInt(strNumSamples), Integer.parseInt(strWeightIdx));
    }
    
    @Override
    public String getInitial() 
    {
      return Initial.class.getName() + getParam();
    }
   
    static public class Initial extends WeightedReservoirSample.Initial
    {        
      public Initial()
      {
          super();
      }
      
      public Initial(String strNumSamples, String strWeightIdx)
      {
          int weightIdx = Integer.parseInt(strWeightIdx);
          int numSamples = Integer.parseInt(strNumSamples);
          super.reservoir = new InverseWeightJumpSampleReservoir(numSamples, weightIdx);
      }
    }
    
    static class InverseWeightJumpSampleReservoir extends ScoredSampleReservoir
    {
        private int weightIdx;
        
        InverseWeightJumpSampleReservoir(int numSamples, int weightIdx) 
        {
            super(numSamples);
            Preconditions.checkArgument(weightIdx >= 0, 
                    "Invalid negative weight field index argument in WeightedReservoirSampleWithExpJump reservoir constructor: " + weightIdx);
            this.weightIdx = weightIdx;
        }
        
        @Override
        ScoreGenerator getScoreGenerator()
        {
            if(this.scoreGen == null) {
                this.scoreGen = new InverseWeightScoreGenerator(this.weightIdx);
            }
            return this.scoreGen;
        }
        
        @Override
        void consider(DataBag samples) throws ExecException 
        {
            ScoreGenerator scoreGen = getScoreGenerator();
        
            double partialWeightSum = 0;
            double reservoirThreshold = 0;
            double r = 0;
            double xw = 0;
            boolean startExpJump = true;
            
            for (Tuple sample : samples) {
              Reservoir reservoir = getReservoir();
              
              if (reservoir.size() < super.getNumSamples()) {
                 reservoir.consider(new ScoredTuple(scoreGen.generateScore(sample), sample));
              } else {
                 if(startExpJump) {
                     partialWeightSum = 0;
                     reservoirThreshold = reservoir.peek().getScore();
                     r = Math.random();
                     /*
                      * Math.log(LOW_EPISLON) = -743.7469247408213
                      * Math.log(HIGH_EPISLON) = -1.1102230246251565E-16
                      * max(xw) = 6.6990767462414305E18 
                      */
                     xw = Math.log(Math.max(r, LOW_EPISLON)) / Math.log(Math.min(Math.max(reservoirThreshold, LOW_EPISLON), HIGH_EPISLON));
                     startExpJump = false;
                 }
                 
                 double weight = ((Number)sample.get(this.weightIdx)).doubleValue();
                 
                 if(Double.compare(weight, 0.0) <= 0)
                 {
                     //non-positive weight should be avoided
                     throw new ExecException(String.format("Invalid sample weight [%f]. It should be a positive real number", weight));
                 }
                 
                 if(Double.compare(partialWeightSum, xw) < 0 && 
                    Double.compare(partialWeightSum + weight, xw) >= 0) {
                    //replace the item with the minimum key with the new item
                    reservoir.poll();
                    double tw = Math.pow(reservoirThreshold, weight);
                    double r2 = tw + (1.0 - tw) * Math.random(); //rand(tw, 1.0)
                    ScoredTuple st = new ScoredTuple(Math.pow(r2, 1/weight), sample);
                    reservoir.consider(st);
                    startExpJump = true;
                 } else {
                    //skip item
                    partialWeightSum += weight;
                 }
               }
            }
        }
    }
}