package datafu.pig.sampling;

import java.util.PriorityQueue;

public class Reservoir extends PriorityQueue<ScoredTuple>
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
