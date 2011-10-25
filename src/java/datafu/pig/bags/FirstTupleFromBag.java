package datafu.pig.bags;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

/* Given a bag, return the first tuple from the bag

For example,
FirstTupleFromBag({(1), (2)}, null) -> (1)
 */

public class FirstTupleFromBag extends EvalFunc<Object>
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  @Override
  public Object exec(Tuple input) throws IOException
  {
    // PigStatusReporter reporter = PigStatusReporter.getInstance();
    try {
      DataBag outputBag = bagFactory.newDefaultBag();
      long i=0, j, cnt=0;
      DataBag inputBag = (DataBag) input.get(0);
      Object default_val = input.get(1);
      for(Tuple bag2tuple : inputBag){
        outputBag.add(bag2tuple);
        return bag2tuple;
      }
      return default_val;
    }
    catch (Exception e) {
      throw WrappedIOException.wrap("Caught exception processing input of " + this.getClass().getName(), e);
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      return new Schema(input.getField(0).schema);
    }
    catch (Exception e) {
      return null;
    }
  }
}

