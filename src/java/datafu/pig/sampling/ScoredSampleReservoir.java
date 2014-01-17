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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.google.common.base.Preconditions;

abstract class ScoredSampleReservoir {
    private Reservoir reservoir;
    
    private int numSamples;
    
    protected ScoreGenerator scoreGen;
    
    protected ScoredSampleReservoir(int numSamples) 
    {
        Preconditions.checkArgument(numSamples > 0, 
                "Invalid non-positive numSamples argument to constructor of class ScoredSampleReservoir or its subclass: " + numSamples);
        this.numSamples = numSamples;
        this.reservoir = null;
        this.scoreGen = null;
    }
    
    Reservoir getReservoir()
    {
        if (this.reservoir == null) {
            this.reservoir = new Reservoir(this.numSamples);
        }
        return this.reservoir;
    }
    
    int getNumSamples()
    {
        return this.numSamples;
    }
    
    void clear()
    {
        this.reservoir = null;
        this.scoreGen = null;
    }
    
    abstract ScoreGenerator getScoreGenerator();
    
    abstract void consider(DataBag samples) throws ExecException;
    
    static interface ScoreGenerator
    {      
        double generateScore(Tuple sample) throws ExecException;
    }
    
    protected static class PureRandomScoreGenerator implements ScoreGenerator
    {
        public PureRandomScoreGenerator(){}
        
        public double generateScore(Tuple sample)
        {
            return Math.random();
        }
    }
    
    protected static class InverseWeightScoreGenerator implements ScoreGenerator
    {        
        //index of the weight field of the input tuple
        private int weightIdx;
        
        InverseWeightScoreGenerator(int weightIdx) 
        {
            //double check the weight index is valid
            Preconditions.checkArgument(weightIdx >= 0, 
                               "Invalid negative weight index argument to InverseWeightScoreGenerator constructor: " + weightIdx);
            this.weightIdx = weightIdx;
        }
        
        @Override
        public double generateScore(Tuple sample) throws ExecException
        {
            if(sample == null) {
                throw new ExecException("Invalid null sample input into method InverseWeightScoreGenerator.generateScore()");
            }
            
            double weight = ((Number)sample.get(this.weightIdx)).doubleValue();
            if(Double.compare(weight, 0.0) <= 0)
            {
                //non-positive weight should be avoided
                throw new ExecException(String.format("Invalid sample weight [%f]. It should be a positive real number", weight));
            }
            
            return Math.pow(Math.random(), 1/weight);
        }
    }
}
