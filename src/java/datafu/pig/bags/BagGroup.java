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

package datafu.pig.bags;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.AliasableEvalFunc;

/**
 * Performs an in-memory group operation on a bag.  The first argument is the bag.
 * The second argument is a projection of that bag to the group keys.
 *
 * <p>
 * Example:
 * <code>
 * define BagGroup datafu.pig.bags.BagGroup();
 * 
 * data = LOAD 'input' AS (input_bag: bag {T: tuple(k: int, v: chararray)});
 * -- ({(1,A),(1,B),(2,A),(2,B),(2,C),(3,A)})
 * 
 * data2 = FOREACH data GENERATE BagGroup(input_bag, input_bag.(k)) as grouped;
 * -- data2: {grouped: {(group: int,input_bag: {T: (k: int,v: chararray)})}}
 * -- ({(1,{(1,A),(1,B)}),(2,{(2,A),(2,B),(2,C)}),(3,{(3,A)})})
 * </code>
 * </p>
 * 
 * @author wvaughan
 *
 */
public class BagGroup extends AliasableEvalFunc<DataBag>
{
  private final String FIELD_NAMES_PROPERTY = "FIELD_NAMES";
  private List<String> fieldNames;
  
  @Override
  public Schema getOutputSchema(Schema input)
  {
    try {
      if (input.size() != 2) {
        throw new RuntimeException(String.format("Expected input of format (BAG, PROJECTED_BAG...). Got %d field.", input.size()));
      }      
      // Expect the first field to be a bag
      FieldSchema bagFieldSchema = input.getField(0);
      if (bagFieldSchema.type != DataType.BAG) {
        throw new RuntimeException(String.format("Expected input of format (BAG, PROJECTED_BAG...). Got %s as first field.", DataType.findTypeName(bagFieldSchema.type)));
      }
      // Expect the second fields to be a projection of the bag
      FieldSchema projectedBagFieldSchema = input.getField(1);
      if (projectedBagFieldSchema.type != DataType.BAG) {
        throw new RuntimeException(String.format("Expected input of format (BAG, PROJECTED_BAG...). Got %s as second field.", DataType.findTypeName(projectedBagFieldSchema.type)));
      }
      
      String bagName = bagFieldSchema.alias;
      // handle named tuples
      if (bagFieldSchema.schema.size() == 1) {
        FieldSchema bagTupleFieldSchema = bagFieldSchema.schema.getField(0);
        if (bagTupleFieldSchema.type == DataType.TUPLE && bagTupleFieldSchema.alias != null) {
          bagName = getPrefixedAliasName(bagName, bagTupleFieldSchema.alias);
        }
      }      
      if (projectedBagFieldSchema.schema.size() == 1) {
        FieldSchema projectedBagTupleFieldSchema = projectedBagFieldSchema.schema.getField(0);
        if (projectedBagTupleFieldSchema.type == DataType.TUPLE && projectedBagTupleFieldSchema.schema != null) {
          projectedBagFieldSchema = projectedBagTupleFieldSchema;
        }
      }
      
      // create the output schema for the 'group'
      // store the field names for the group keys
      Schema groupTupleSchema = new Schema();
      fieldNames = new ArrayList<String>(projectedBagFieldSchema.schema.size());
      for (int i=0; i<projectedBagFieldSchema.schema.size(); i++) {
        FieldSchema fieldSchema = projectedBagFieldSchema.schema.getField(i);
        String fieldName = fieldSchema.alias;
        fieldNames.add(getPrefixedAliasName(bagName, fieldName));
        groupTupleSchema.add(new FieldSchema(fieldSchema.alias, fieldSchema.type));
      }
      getInstanceProperties().put(FIELD_NAMES_PROPERTY, fieldNames);
      
      Schema outputTupleSchema = new Schema();
      if (projectedBagFieldSchema.schema.size() > 1) {
        // multiple group keys
        outputTupleSchema.add(new FieldSchema("group", groupTupleSchema, DataType.TUPLE));
      } else {
        // single group key
        outputTupleSchema.add(new FieldSchema("group", groupTupleSchema.getField(0).type));
      }
      outputTupleSchema.add(bagFieldSchema);      
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    } catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
  
  Map<Tuple, List<Tuple>> groups = new HashMap<Tuple, List<Tuple>>();
  TupleFactory tupleFactory = TupleFactory.getInstance();
  BagFactory bagFactory = BagFactory.getInstance();

  @SuppressWarnings("unchecked")
  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    fieldNames = (List<String>)getInstanceProperties().get(FIELD_NAMES_PROPERTY);
    
    DataBag inputBag = (DataBag)input.get(0);    
    
    for (Tuple tuple : inputBag) {
      Tuple key = extractKey(tuple);
      addGroup(key, tuple);
    }
    
    DataBag outputBag = bagFactory.newDefaultBag();
    for (Tuple key : groups.keySet()) {
      Tuple outputTuple = tupleFactory.newTuple();
      if (fieldNames.size() > 1) {
        outputTuple.append(key);
      } else {        
        outputTuple.append(key.get(0));
      }
      DataBag groupBag = bagFactory.newDefaultBag();
      for (Tuple groupedTuple : groups.get(key)) {
        groupBag.add(groupedTuple);
      }
      outputTuple.append(groupBag);
      outputBag.add(outputTuple);
    }
    
    return outputBag;
  }
  
  private Tuple extractKey(Tuple tuple) throws ExecException {
    Tuple key = tupleFactory.newTuple();
    for (String field : fieldNames) {
      key.append(getObject(tuple, field));
    }
    return key;
  }
  
  private void addGroup(Tuple key, Tuple value) {
    if (!groups.containsKey(key)) {
      groups.put(key, new LinkedList<Tuple>());
    }
    groups.get(key).add(value);
  }

}
