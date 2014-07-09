package datafu.pig.stats;


import datafu.pig.util.RangeMap;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Format for configuration strings is NumericalRange : Value
 */
public class RangeScoringFunction implements PositionScoringFunction
{
  private final RangeMap<Double> rangeMap;

  public RangeScoringFunction(String... configuration)
  {
    final List<Map.Entry<String, Double>> ranges = new ArrayList<Map.Entry<String, Double>>();

    for(String config : configuration)
    {
      String[] split = config.split(":");
      if(split.length == 2)
      {
        ranges.add(new AbstractMap.SimpleEntry<String, Double>(split[0].trim(), Double.parseDouble(split[1])));
      }
      else
      {
        throw new IllegalArgumentException("Format for range discounting function should be NumericalRange : Value");
      }
    }

    this.rangeMap = new RangeMap(ranges);
  }

  @Override
  public double score(int position)
  {
    return rangeMap.get(position);
  }
}
