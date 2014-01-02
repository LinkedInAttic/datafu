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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

class ScoredTuple implements Comparable<ScoredTuple>
{
  Double score;
  private Tuple tuple;
  
  public ScoredTuple()
  {
    
  }
                     
  public ScoredTuple(Double score, Tuple tuple)
  {
    this.score = score;       
    this.setTuple(tuple);
  }
  
  public Double getScore() {
    return score;
  }

  public void setScore(Double score) {
    this.score = score;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public void setTuple(Tuple tuple) {
    this.tuple = tuple;
  }
  
  public Tuple getIntermediateTuple(TupleFactory tupleFactory)
  {
    Tuple intermediateTuple = tupleFactory.newTuple(2);
    try {
      intermediateTuple.set(0, score);
      intermediateTuple.set(1, tuple);
    }
    catch (ExecException e) {
      throw new RuntimeException(e);
    }
    
    return intermediateTuple;
  }
  
  public static ScoredTuple fromIntermediateTuple(Tuple intermediateTuple) throws ExecException
  {
    //Double score = ((Number)intermediateTuple.get(0)).doubleValue();
    try {
    Double score = (Double)intermediateTuple.get(0);
    Tuple originalTuple = (Tuple)intermediateTuple.get(1);
    return new ScoredTuple(score, originalTuple);
    } catch (Exception e) {
      throw new RuntimeException("Cannot deserialize intermediate tuple: "+intermediateTuple.toString(), e);
    }
  }

  @Override
  public int compareTo(ScoredTuple o) {
    if (score == null) {
      if (o == null) return 0;
      else return -1;
    }
    return score.compareTo(o.score);
  }    
}
