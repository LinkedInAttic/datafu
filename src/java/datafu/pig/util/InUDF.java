package datafu.pig.util;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * Similar to the SQL IN function, this function provides a convenient way to filter 
 * using a logical disjunction over many values. 
 * Returns true when the first value of the tuple is contained within the remainder of the tuple.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define In datafu.pig.util.InUDF();
 * -- cars: (alice, red), (bob, blue), (charlie, green), (dave, red);
 * cars = LOAD cars AS (owner:chararray, color:chararray);
 * 
 * -- cars: (alice, red), (bob, blue), (dave, red);
 * red_blue_cars = FILTER cars BY In(color, 'red', 'blue');
 * 
 * }</pre>
 * </p>
 * 
 * @author wvaughan
 *
 */
public class InUDF extends FilterFunc
{

  @Override
  public Boolean exec(Tuple input) throws IOException
  {
    Object o = input.get(0);
    Boolean match = false;
    if (o != null) {
      for (int i=1; i<input.size() && !match; i++) {
        match = match || o.equals(input.get(i));
      }
    }    
    return match;
  }

}
