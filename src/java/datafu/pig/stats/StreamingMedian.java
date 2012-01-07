package datafu.pig.stats;

/**
 * Computes approximate median for a (not necessarily sorted) input bag, using the
 * Munro-Paterson algorithm for approximating quantiles.  This is just a wrapper
 * around StreamingQuantile.
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