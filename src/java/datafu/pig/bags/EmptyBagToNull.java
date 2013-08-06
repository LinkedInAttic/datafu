package datafu.pig.bags;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Returns null if the input is an empty bag; otherwise,
 * returns the input bag unchanged.
 */
public class EmptyBagToNull extends EvalFunc<DataBag>
{
  @Override
  public DataBag exec(Tuple tuple) throws IOException
  {
    if (tuple.size() == 0 || tuple.get(0) == null)
      return null;
    Object o = tuple.get(0);
    if (o instanceof DataBag)
    {
      DataBag bag = (DataBag)o;
      if (bag.size() == 0)
      {
        return null;
      }
      else
      {
        return bag;
      }
    }
    else
      throw new IllegalArgumentException("expected a null or a bag");
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return input;
  }
}