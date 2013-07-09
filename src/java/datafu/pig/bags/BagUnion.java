package datafu.pig.bags;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BagUnion extends EvalFunc<DataBag>
{

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    DataBag outerBag = (DataBag)input.get(0);
    DataBag output = BagFactory.getInstance().newDefaultBag();
    for (Tuple outerTuple : outerBag) {
      DataBag innerBag = (DataBag)outerTuple.get(0);
      for (Tuple innerTuple : innerBag) {
        output.add(innerTuple);
      }
    }
    return output;
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      // verify that the input tuple contains only a bag
      if (input.size() != 1) {
        throw new RuntimeException(String.format(
                                                 "Expected input to have only a single field.  Got instead: %s", 
                                                 input.prettyPrint()));
      }
      if (input.getField(0).type != DataType.BAG) {
        throw new RuntimeException(String.format(
                                                 "Expected a BAG of BAGs as input. Got instead: %s",
                                                 DataType.findTypeName(input.getField(0).type)));
      }
      
      // take the tuple schema from this outer bag
      Schema outerBagTuple = input.getField(0).schema;      
      // verify that this tuple contains only a bag for each element
      Schema outerBagSchema = outerBagTuple.getField(0).schema;      
      if (outerBagSchema.size() != 1) {
        throw new RuntimeException(String.format(
                                                 "Expected outer bag to have only a single field.  Got instead: %s", 
                                                 outerBagSchema.prettyPrint()));
      }
      if (outerBagSchema.getField(0).type != DataType.BAG) {
        throw new RuntimeException(String.format(
                                                 "Expected a BAG of BAGs as input.  Got instead: %s", 
                                                 DataType.findTypeName(outerBagSchema.getField(0).type)));
      }
      
      // take the schema of the inner tuple as the schema for the tuple in the output bag
      Schema innerTupleSchema = outerBagSchema.getField(0).schema;
    
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               innerTupleSchema, DataType.BAG));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
