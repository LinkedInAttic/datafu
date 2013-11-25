/*
 * Copyright 2013 LinkedIn Corp. and contributors
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
