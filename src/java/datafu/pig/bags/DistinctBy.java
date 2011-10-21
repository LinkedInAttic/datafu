package datafu.pig.bags;

import java.io.IOException;
import java.util.HashSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Get distinct elements in a bag by a given set of field positions.
 * The input and output schemas will be identical.  
 * 
 * The first tuple containing each distinct combination of these fields will be taken.
 * 
 * This operation is order preserving.  If both A and B appear in the output,
 * and A appears before B in the input, then A will appear before B in the output.
 * 
 * Example:
 * <pre>
 * {@code
 * define DistinctBy datafu.pig.bags.DistinctBy('0');
 * 
 * -- input:
 * -- ({(a, 1),(a,1),(b, 2),(b,22),(c, 3),(d, 4)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 * 
 * output = FOREACH input GENERATE DistinctBy(B);
 * 
 * -- output:
 * -- ({(a,1),(b,2),(c,3),(d,4)})
 * } 
 * </pre>
 * 
 * @param map Any number of strings specifying field positions
 */
public class DistinctBy extends EvalFunc<DataBag>
{
  private final static String delimiter = "-";
  private HashSet<Integer> fields = new HashSet<Integer>();
  
  public DistinctBy(String... fields)
  {
    for(String field : fields) {
      this.fields.add(Integer.parseInt(field));
    }   
  }


  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    if (input.size() != 1) {
      throw new RuntimeException("Expected input to have only a single field");
    }    
    if (input.getType(0) != DataType.BAG) {
      throw new RuntimeException("Expected a BAG as input");
    }
    // new hash to record things that have already been seen
    HashSet<String> seen = new HashSet<String>();    

    DataBag inputBag = (DataBag)input.get(0);
    DataBag outputBag = BagFactory.getInstance().newDefaultBag();
    for (Tuple t : inputBag) {
      String distinctString = getDistinctString(t, this.fields);
      if (!seen.contains(distinctString)) {
        outputBag.add(t);
        seen.add(distinctString);
      }
    }
    return outputBag;    
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
      
      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      Schema outputTupleSchema = inputTupleSchema.clone();     
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
  
  private String getDistinctString(Tuple t, HashSet<Integer> distinctFieldPositions) throws ExecException {
    String[] tokens = t.toDelimitedString(delimiter).split(delimiter);
    StringBuffer buffer = new StringBuffer();
    for(int i=0; i<tokens.length; i++) {
      if (distinctFieldPositions.contains(i)) {
        buffer.append(tokens[i]);
        buffer.append(delimiter);
      }
    }
    buffer.substring(0, buffer.length() - delimiter.length());
    return buffer.toString();
  }

}
