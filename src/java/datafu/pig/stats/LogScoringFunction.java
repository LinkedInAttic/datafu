package datafu.pig.stats;


/**
 * Standard NDCG log discounting
 */
public class LogScoringFunction implements PositionScoringFunction
{
  private static final double LOG2 = Math.log(2);

  @Override
  public double score(int position)
  {
    return position < 1 ? 1 : LOG2 / Math.log(2 + position);
  }
}
