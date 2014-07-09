package datafu.pig.stats;


public class UnaryScoringFunction implements PositionScoringFunction
{
  public UnaryScoringFunction(String... args)
  {
  }

  @Override
  public double score(int position)
  {
    return 1.0;
  }
}
