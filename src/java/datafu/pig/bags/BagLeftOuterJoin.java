/*
 * Copyright 2012 LinkedIn Corp. and contributors
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
 * Performs an in-memory left outer join across multiple bags.
 * 
 * <p>
 * The format for invocation is BagLeftOuterJoin(bag, 'key',....).  
 * This UDF expects that all bags are non-null and that there is a corresponding key for each bag.  
 * The <em>key</em> that is expected is the alias of the key inside of the preceding bag.
 * </p> 
 * 
 * <p>
 * Example:
 * <code>
 * define BagLeftOuterJoin datafu.pig.bags.BagLeftOuterJoin();
 * 
 * -- describe data: 
 * -- data: {bag1: {(key1: chararray,value1: chararray)},bag2: {(key2: chararray,value2: int)}} 
 * 
 * bag_joined = FOREACH data GENERATE BagLeftOuterJoin(bag1, 'key1', bag2, 'key2') as joined;
 * 
 * -- describe bag_joined:
 * -- bag_joined: {joined: {(bag1::key1: chararray, bag1::value1: chararray, bag2::key2: chararray, bag2::value2: int)}} 
 * </code>
 * </p>
 * 
 * @author wvaughan
 * 
 */
public class BagLeftOuterJoin extends AliasableEvalFunc<DataBag>
{

  private static final String BAG_NAMES_PROPERTY = "BagLeftOuterJoin_BAG_NAMES";
  private static final String BAG_NAME_TO_JOIN_PREFIX_PROPERTY = "BagLeftOuterJoin_BAG_NAME_TO_JOIN_PREFIX";
  private static final String BAG_NAME_TO_SIZE_PROPERTY = "BagLeftOuterJoin_BAG_NAME_TO_SIZE_PROPERTY";
  
  ArrayList<String> bagNames;
  Map<String, String> bagNameToJoinKeyPrefix;  
  Map<String, Integer> bagNameToSize;
  
  public BagLeftOuterJoin() {
    
  }
  
  @SuppressWarnings("unchecked")
  private void retrieveContextValues()
  {
    Properties properties = getInstanceProperties();   
    bagNames = (ArrayList<String>) properties.get(BAG_NAMES_PROPERTY);    
    bagNameToJoinKeyPrefix = (Map<String, String>) properties.get(BAG_NAME_TO_JOIN_PREFIX_PROPERTY);
    bagNameToSize = (Map<String, Integer>) properties.get(BAG_NAME_TO_SIZE_PROPERTY);
  }
  
  class JoinCollector
  {
    HashMap<Object, List<Tuple>> joinData;
    
    public void printJoinData() throws ExecException {
      printData(joinData);
    }
    
    public void printData(HashMap<Object, List<Tuple>> data) throws ExecException {
      for (Object o : data.keySet()) {
        System.out.println(o);
        for (Tuple t : data.get(o)) {
          System.out.println("\t"+t.toDelimitedString(", "));
        }
      }
    }
    
    public HashMap<Object, List<Tuple>> groupTuples(Iterable<Tuple> tuples, String keyName) throws ExecException {
      HashMap<Object, List<Tuple>> group = new HashMap<Object, List<Tuple>>();
      for (Tuple tuple : tuples) {
        Object key = getObject(tuple, keyName);
        if (!group.containsKey(key)) {
          group.put(key, new LinkedList<Tuple>());
        }
        group.get(key).add(tuple);
      }
      return group;
    }
    
    public HashMap<Object, List<Tuple>> insertNullTuples(HashMap<Object, List<Tuple>> groupedData, int tupleSize) throws ExecException {
      Tuple nullTuple = TupleFactory.getInstance().newTuple(tupleSize);
      for (int i=0; i<tupleSize; i++) {
        nullTuple.set(i, null);
      }     
      for (Object key : joinData.keySet()) {
        if (!groupedData.containsKey(key)) {
          groupedData.put(key, Collections.singletonList(nullTuple));
        }
      }
      return groupedData;      
    }
    
    public void joinTuples(Object key, List<Tuple> tuples) throws ExecException {
      List<Tuple> currentTuples = joinData.get(key);
      if (currentTuples != null) {
        List<Tuple> newTuples = new LinkedList<Tuple>();
        if (tuples != null) {
          for (Tuple t1 : currentTuples) {
            for (Tuple t2 : tuples) {
              Tuple t = TupleFactory.getInstance().newTuple();
              for (Object o : t1.getAll()) {
                t.append(o);
              }
              for (Object o : t2.getAll()) {
                t.append(o);
              }
              newTuples.add(t);
            }
          }
        }
        joinData.put(key, newTuples);
      }      
    }    
    
    public HashMap<Object, List<Tuple>> getJoinData() {
      return this.joinData;
    }
    
    public void setJoinData(HashMap<Object, List<Tuple>> joinData) {
      this.joinData = joinData;
    }
  }

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    retrieveContextValues();

    ArrayList<String> joinKeyNames = new ArrayList<String>();
    for (int i = 1; i < input.size(); i += 2) {
      joinKeyNames.add((String) input.get(i));
    }
    
    JoinCollector collector = new JoinCollector();
    // the first bag is the outer bag
    String leftBagName = bagNames.get(0);
    DataBag leftBag = getBag(input, leftBagName);
    String leftBagJoinKeyName = getPrefixedAliasName(bagNameToJoinKeyPrefix.get(leftBagName), joinKeyNames.get(0));    
    collector.setJoinData(collector.groupTuples(leftBag, leftBagJoinKeyName));
    // now, for each additional bag, group up the tuples by the join key, then join them in
    if (bagNames.size() > 1) {
      for (int i = 1; i < bagNames.size(); i++) {
        String bagName = bagNames.get(i);
        DataBag bag = getBag(input, bagName);
        String joinKeyName = getPrefixedAliasName(bagNameToJoinKeyPrefix.get(bagName), joinKeyNames.get(i));
        int tupleSize = bagNameToSize.get(bagName);
        if (bag == null) throw new IOException("Error in instance: "+getInstanceName()
                + " with properties: " + getInstanceProperties()
                + " and tuple: " + input.toDelimitedString(", ")
                + " -- Expected bag, got null");
        HashMap<Object, List<Tuple>> groupedData = collector.groupTuples(bag, joinKeyName);
        // outer join, so go back in and add nulls;
        groupedData = collector.insertNullTuples(groupedData, tupleSize);
        for (Map.Entry<Object, List<Tuple>> entry : groupedData.entrySet()) {
          collector.joinTuples(entry.getKey(), entry.getValue());          
        }
      }      
    }
    
    // assemble output bag
    DataBag outputBag = BagFactory.getInstance().newDefaultBag();
    for (List<Tuple> tuples : collector.getJoinData().values()) {
      for (Tuple tuple : tuples) {
        outputBag.add(tuple);
      }
    }

    return outputBag;
  }

  @Override
  public Schema getOutputSchema(Schema input)
  {
    ArrayList<String> bagNames = new ArrayList<String>(input.size() / 2);
    Map<String, String> bagNameToJoinPrefix = new HashMap<String, String>(input.size() / 2);
    Map<String, Integer> bagNameToSize = new HashMap<String, Integer>(input.size() / 2);
    Schema outputSchema = null;
    Schema bagSchema = new Schema();
    try {
      int i = 0;
      // all even fields should be bags, odd fields are key names
      String bagName = null;
      String tupleName = null;
      for (FieldSchema outerField : input.getFields()) {
        if (i++ % 2 == 1)
          continue;
        bagName = outerField.alias;
        bagNames.add(bagName);
        if (bagName == null)
          bagName = "null";
        if (outerField.schema == null)
          throw new RuntimeException("Expected input format of (bag, 'field') pairs. "
              +"Did not receive a bag at index: "+i+", alias: "+bagName+". "
              +"Instead received type: "+DataType.findTypeName(outerField.type)+" in schema:"+input.toString());
        FieldSchema tupleField = outerField.schema.getField(0);
        tupleName = tupleField.alias;
        bagNameToJoinPrefix.put(bagName, getPrefixedAliasName(outerField.alias, tupleName));
        if (tupleField.schema == null) {
          log.error(String.format("could not get schema for inner tuple %s in bag %s", tupleName, bagName));
        } else {          
          bagNameToSize.put(bagName, tupleField.schema.size());
          for (FieldSchema innerField : tupleField.schema.getFields()) {
            String innerFieldName = innerField.alias;
            if (innerFieldName == null)
              innerFieldName = "null";
            String outputFieldName = bagName + "::" + innerFieldName;
            bagSchema.add(new FieldSchema(outputFieldName, innerField.type));
          }
        }
      }
      outputSchema = new Schema(new Schema.FieldSchema("joined", bagSchema, DataType.BAG));
      log.debug("output schema: "+outputSchema.toString());
    } catch (FrontendException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    Properties properties = getInstanceProperties();
    properties.put(BAG_NAMES_PROPERTY, bagNames);
    properties.put(BAG_NAME_TO_JOIN_PREFIX_PROPERTY, bagNameToJoinPrefix);
    properties.put(BAG_NAME_TO_SIZE_PROPERTY, bagNameToSize);
    return outputSchema;
  }

}

