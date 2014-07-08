package datafu.pig.stats;


/**
 * Interface for abstracting discounting according to a position within a list from other algorithms
 */
public interface PositionScoringFunction
{
  /**
   * @param position The 0-based index to score
   * @return
   */
  double score(int position);
}
