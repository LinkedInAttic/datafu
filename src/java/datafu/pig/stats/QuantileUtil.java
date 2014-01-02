/*
 * Copyright 2012 LinkedIn Corp. and contributors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package datafu.pig.stats;

import java.util.ArrayList;

/**
 * Methods used by {@link Quantile}.
 * 
 * @author "Matthew Hayes <mhayes@linkedin.com>"
 *
 */
public class QuantileUtil
{ 
  public static ArrayList<Double> getNQuantiles(int numQuantiles)
  {
    ArrayList<Double> quantiles = new ArrayList<Double>(numQuantiles);
    quantiles = new ArrayList<Double>(numQuantiles);
    int divisor = numQuantiles-1;
    for (int q = 0; q <= divisor; q++)
    {
      double quantile = ((double)q)/divisor;
      quantiles.add(quantile);
    }
    return quantiles;
  }
  
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
      
      quantiles = getNQuantiles(numQuantiles);
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
