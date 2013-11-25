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

package datafu.pig.sampling;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * Provides a way of sampling tuples based on certain fields.
 * This is essentially equivalent to grouping on the fields, applying SAMPLE,
 * and then flattening.  It is much more efficient though because it does not require
 * a reduce step.
 * 
 * <p>
 * The method of sampling is to convert the key to a hash, derive a double value
 * from this, and then test this against a supplied probability.  The double value
 * derived from a key is uniformly distributed between 0 and 1.
 * </p>
 * 
 * <p>
 * The only required parameter is the sampling probability.  This may be followed
 * by an optional seed value to control the random number generation.  
 * </p>
 * 
 * <p>
 * SampleByKey will work deterministically as long as the same seed is provided.  
 * </p>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE SampleByKey datafu.pig.sampling.SampleByKey('0.5');
 * 
 *-- input: (A,1), (A,2), (A,3), (B,1), (B,3)
 * 
 * data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
 * output = FILTER data BY SampleByKey(A_id);
 * 
 * --output: (B,1), (B,3)
 * } 
 * 
 * </pre>
 * </p>
 * @author evion 
 * 
 */

public class SampleByKey extends FilterFunc
{
  final static int PRIME_NUMBER = 31;
  
  Integer seed = null;
  double probability;
  
  public SampleByKey(String probability) {
    this.probability = Double.parseDouble(probability);
  }
  
  public SampleByKey(String probability, String salt) {
    this(probability);
    this.seed = salt.hashCode();
  }

  @Override
  public void setUDFContextSignature(String signature)
  {
    if (this.seed == null)
      this.seed = signature.hashCode();
    super.setUDFContextSignature(signature);
  }

  @Override
  public Boolean exec(Tuple input) throws IOException 
  {
    int hashCode = 0;
    for(int i=0; i<input.size(); i++) {
      Object each = input.get(i);
      hashCode = hashCode*PRIME_NUMBER + each.hashCode();
    }
      
    try {
      return intToRandomDouble(hashCode) <= probability;
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception on intToRandomDouble");
    }
  }
  
  private Double intToRandomDouble(int input) throws Exception
  {
    MessageDigest hasher = MessageDigest.getInstance("sha-1");

    ByteBuffer b = ByteBuffer.allocate(4+4);
    ByteBuffer b2 = ByteBuffer.allocate(20);

    b.putInt(seed);
    b.putInt(input);
    byte[] digest = hasher.digest(b.array());
    b.clear();

    b2.put(digest);
    b2.rewind();
    double result = (((double)b2.getInt())/Integer.MAX_VALUE  + 1)/2;
    b2.clear();

    return result;
  }
}
