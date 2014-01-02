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

import java.util.PriorityQueue;

class Reservoir extends PriorityQueue<ScoredTuple>
{
  private static final long serialVersionUID = 1L;
  private int numSamples;
  
  public Reservoir(int numSamples)
  {
    super(numSamples);
    this.numSamples = numSamples;
  }
  
  public boolean consider(ScoredTuple scoredTuple)
  {
    if (super.size() < numSamples) {
      return super.add(scoredTuple);
    } else {      
      ScoredTuple head = super.peek();
      if (scoredTuple.score > head.score) {
        super.poll();
        return super.add(scoredTuple);
      }
      return false;
    }
  }
}
