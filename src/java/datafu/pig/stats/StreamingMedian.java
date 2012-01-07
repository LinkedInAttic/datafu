package datafu.pig.stats;

/**
 * Computes the approximate {@link <a href="http://en.wikipedia.org/wiki/Median" target="_blank">median</a>} 
 * for a (not necessarily sorted) input bag, using the Munro-Paterson algorithm.  
 * This is a convenience wrapper around StreamingQuantile.
 *
 * <p>
 * N.B., all the data is pushed to a single reducer per key, so make sure some partitioning is 
 * done (e.g., group by 'day') if the data is too large.  That is, this isn't distributed median.
 * </p>
 * 
 * @see StreamingQuantile
 */
public class StreamingMedian extends StreamingQuantile
{
  public StreamingMedian()
  {
    super("0.5");
  }
}