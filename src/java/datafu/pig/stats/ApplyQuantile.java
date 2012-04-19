package datafu.pig.stats;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

public class ApplyQuantile extends SimpleEvalFunc<Double>
{
  private List<Double> quantiles;
  
  public ApplyQuantile(String... k)
  {
    this.quantiles = QuantileUtil.getQuantilesFromParams(k);
  }
  
  public Double call(Double value, Tuple quantiles) throws IOException
  {
    // TODO do a binary search instead in case there are many quantiles
    for (int i=quantiles.size()-1; i>=0 ; i--)
    {
      Double quantile = (Double)quantiles.get(i);
      if (value >= quantile)
      {
        return (Double)this.quantiles.get(i);
      }
    }
    
    return this.quantiles.get(0);
  }
}
