package datafu.pig.stats;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Given a set of pre-computed quantiles, converts a value to the quantile it belongs to.
 * <p>
 * Quantiles can be computed with either Quantile or StreamingQuantile.
 * </p>
 * 
 * @author "Matt Hayes <mhayes@linkedin.com>"
 *
 * @see Quantile
 * @see StreamingQuantile
 */
public class ApplyQuantile extends SimpleEvalFunc<Double>
{
  private List<Double> quantiles;
  
  public ApplyQuantile(String... k)
  {
    this.quantiles = QuantileUtil.getQuantilesFromParams(k);
  }
  
  public Double call(Double value, Tuple quantilesComputed) throws IOException
  {
    if (quantilesComputed.size() != quantiles.size())
    {
      throw new IOException("Expected computed quantiles to have size " + quantiles.size()
                            + " but found quantiles with size " + quantilesComputed.size());
    }
    return findQuantile(value,quantilesComputed);
  }
  
  private Double findQuantile(Double value, Tuple quantilesComputed) throws ExecException
  {
    int min = 0;
    int max = this.quantiles.size()-1;
    
    if (value <= (Double)quantilesComputed.get(min))
    {
      return this.quantiles.get(min);
    }
    
    if (value >= (Double)quantilesComputed.get(max))
    {
      return this.quantiles.get(max);
    }
    
    // find the two quantiles which bound the value, then return the lesser quantile
    while (true)
    {
      if (max-min == 1)
      {
        return this.quantiles.get(min);
      }
      
      int middle = (max-min)/2 + min;
      if ((Double)quantilesComputed.get(middle) > value)
      {
        max = middle;
      }
      else
      {
        min = middle;
      }
    }
  }
}
