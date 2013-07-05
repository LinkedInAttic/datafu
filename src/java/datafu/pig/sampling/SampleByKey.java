package datafu.pig.sampling;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * 
 * SampleByKey provides easier way of sampling tuples based on certain key field.
 * 
 * Optional first parameter is salt that will be used for generate seed.
 * Second parameter is sampling probability.
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * DEFINE SampleByKey datafu.pig.sampling.SampleByKey('salt1.5', '0.5');
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
 * 
 * @author evion 
 * 
 */

public class SampleByKey extends FilterFunc
{
  final static String DEFAULT_SEED = "323148";
  final static int PRIME_NUMBER = 31;
  
  int seed;
  double size;
  
  public SampleByKey(String size){
    this(DEFAULT_SEED, size);
  }
  
  public SampleByKey(String salt, String size){
    this.seed = salt.hashCode(); 
    this.size = Double.parseDouble(size);
  }
  
  @Override
  public Boolean exec(Tuple input) throws IOException 
  {    
    int hashCode = 0;
    for(Object each:input){
      hashCode = hashCode*PRIME_NUMBER + each.hashCode();
    }
    
    try {
      if (intToRandomDouble(hashCode) <= size) return true;
      return false;
    } catch (Exception e){
        e.printStackTrace(); 
        throw new RuntimeException("Exception on intToRandomDouble");
    }
  }
  
  private Double intToRandomDouble(int input) throws Exception{
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
