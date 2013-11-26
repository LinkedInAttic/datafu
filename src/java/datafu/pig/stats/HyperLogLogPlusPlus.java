package datafu.pig.stats;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF that applies the HyperLogLog++ cardinality estimation algorithm.
 * 
 * <p>
 * This uses the implementation of HyperLogLog++ from <a href="https://github.com/addthis/stream-lib" target="_blank">stream-lib</a>.
 * The HyperLogLog++ algorithm is an enhanced version of HyperLogLog as described in 
 * <a href="http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf">here</a>.
 * </p>
 * 
 * <p>
 * This is a streaming implementation, and therefore the input data does not need to be sorted.
 * </p>
 * 
 * @author mhayes
 *
 */
public class HyperLogLogPlusPlus extends AccumulatorEvalFunc<Long>
{
  private com.clearspring.analytics.stream.cardinality.HyperLogLogPlus estimator;
  
  private final int p;
  
  /**
   * Constructs a HyperLogLog++ estimator.
   */
  public HyperLogLogPlusPlus()
  {
    this("20");
  }
  
  /**
   * Constructs a HyperLogLog++ estimator.
   * 
   * @param p precision value
   */
  public HyperLogLogPlusPlus(String p)
  {
    this.p = Integer.parseInt(p);
    cleanup();
  }
  
  @Override
  public void accumulate(Tuple arg0) throws IOException
  {
    DataBag inputBag = (DataBag)arg0.get(0);
    for (Tuple t : inputBag) 
    {
      estimator.offer(t);
    }
  }

  @Override
  public void cleanup()
  {
    this.estimator = new com.clearspring.analytics.stream.cardinality.HyperLogLogPlus(p);
  }

  @Override
  public Long getValue()
  {
    return this.estimator.cardinality();
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have only a single field");
      }
      
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
}
