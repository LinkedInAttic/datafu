package datafu.pig.stats;

import java.util.ArrayList;

public class QuantileUtil
{ 
  public static ArrayList<Double> getQuantilesFromParams(String... k)
  {
    ArrayList<Double> quantiles = new ArrayList<Double>(k.length);
    for (String s : k) { 
      quantiles.add(Double.parseDouble(s));
    }
    
    if (quantiles.size() == 1 && quantiles.get(0) > 1.0)
    {
      int numQuantiles = Integer.parseInt(k[0]);
      if (numQuantiles < 1)
      {
        throw new IllegalArgumentException("Number of quantiles must be greater than 1");
      }
      
      quantiles = new ArrayList<Double>(numQuantiles);
      for (double d = 0.0; d <= 1.0; d += 1.0/(numQuantiles-1))
      {
        quantiles.add(d);
      }
    }
    else
    {
      for (Double d : quantiles)
      {
        if (d < 0.0 || d > 1.0)
        {
          throw new IllegalArgumentException("Quantile must be between 0.0 and 1.0");
        }
      }
    }
    
    return quantiles;
  }
}
