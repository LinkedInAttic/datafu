package datafu.pig.stats;

/**
 * Computes the approximate median for a (not necessarily sorted) input bag, using the
 * Munro-Paterson algorithm.  This is a convenience wrapper around StreamingQuantile.
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