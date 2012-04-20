package datafu.pig.stats;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Given a set of pre-computed quantiles, converts a value to the quantile it belongs to.
 * It accepts two parameters.  The first is a tuple, where the first element is the value to
 * convert.  The second is a tuple of computed quantiles.  It returns the first tuple, with
 * the value to convert being replaced by the quantile it belongs to.
 * <p>
 * Quantiles can be computed with either Quantile or StreamingQuantile.
 * </p>
 * 
 * @author "Matt Hayes <mhayes@linkedin.com>"
 *
 * @see Quantile
 * @see StreamingQuantile
 */
public class ApplyQuantile extends SimpleEvalFunc<Tuple>
{
  private List<Double> quantiles;
  
  public ApplyQuantile(String... k)
  {
    this.quantiles = QuantileUtil.getQuantilesFromParams(k);
  }
  
  public Tuple call(Tuple value, Tuple quantilesComputed) throws IOException
  {
    if (quantilesComputed.size() != quantiles.size())
    {
      throw new IOException("Expected computed quantiles to have size " + quantiles.size()
                            + " but found quantiles with size " + quantilesComputed.size());
    }
    
    Double quantileValue = findQuantile((Double)value.get(0),quantilesComputed);
    value.set(0, quantileValue);
    
    return value;
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

  @Override
  public Schema outputSchema(Schema input)
  {
    try
    {
      FieldSchema fieldSchema = input.getField(0);
      if (fieldSchema.type != DataType.TUPLE)
      {
        throw new RuntimeException("Expected a tuple");
      }
      
      if (fieldSchema.schema.getField(0).type != DataType.DOUBLE)
      {
        throw new RuntimeException("Expected a double");
      }
      
      return fieldSchema.schema;
    }
    catch (FrontendException e)
    {
      throw new RuntimeException(e);
    }
  }
}
