package datafu.pig.sampling;

import java.io.IOException;
import java.util.Random;
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
 * 
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
    Random generator = new Random(seed * hashCode);
    double value = generator.nextDouble();
    if (value <= size) return true;
    return false;
  }
}
