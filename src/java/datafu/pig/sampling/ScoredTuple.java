package datafu.pig.sampling;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ScoredTuple implements Comparable<ScoredTuple>
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
