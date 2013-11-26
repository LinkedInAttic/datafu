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

package datafu.pig.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Makes implementing and using UDFs easier by enabling named parameters. 
 * 
 * <p>
 * This works by capturing the schema of the input tuple on the front-end and storing it into the UDFContext. 
 * It provides an easy means of referencing the parameters on the back-end to aid in writing schema-based UDFs.
 * </p>
 * 
 * <p>
 * A related class is {@link SimpleEvalFunc}.  However they are actually fairly different.  The primary purpose of {@link SimpleEvalFunc} is 
 * to skip the boilerplate under the assumption that the arguments in and out are well... simple.  
 * It also assumes that these arguments are in a well-defined positional ordering.
 * </p>
 * 
 * <p>
 * AliasableEvalFunc allows the UDF writer to avoid dealing with all positional assumptions and instead reference fields 
 * by their aliases.  This practice allows for more readable code since the alias names should have more meaning 
 * to the reader than the position.  This approach is also less error prone since it creates a more explicit contract 
 * for what input the UDF expects and prevents simple mistakes that positional-based UDFs could not easily catch, 
 * such as transposing two fields of the same type.  If this contract is violated, say, by attempting to reference 
 * a field that is not present, a meaningful error message may be thrown.
 * </p>
 * 
 * <p>
 * Example:  This example computes the monthly payments for mortgages depending on interest rate.
 * <pre>
 * {@code
 *  public class MortgagePayment extends AliasableEvalFunc<DataBag> {
 *    ...
 *    public DataBag exec(Tuple input) throws IOException {
 *      DataBag output = BagFactory.getInstance().newDefaultBag();
 *      
 *      Double principal = getDouble(input, "principal"); // get a value from the input tuple by alias
 *      Integer numPayments = getInteger(input, "num_payments");
 *      DataBag interestRates = getBag(input, "interest_rates");
 *    
 *      for (Tuple interestTuple : interestRates) {
 *        Double interest = getDouble(interestTuple, getPrefixedAliasName("interest_rates", "interest_rate"));  // get a value from the inner bag tuple by alias
 *        double monthlyPayment = computeMonthlyPayment(principal, numPayments, interest);
 *        output.add(TupleFactory.getInstance().newTuple(monthlyPayment));
 *      }
 *      return output;
 *    }
 *  }
 * }
 * </pre>
 * </p>
 * 
 * @author wvaughan
 *
 * @param <T>
 */
public abstract class AliasableEvalFunc<T> extends ContextualEvalFunc<T>
{
  private static final String ALIAS_MAP_PROPERTY = "aliasMap";
    
  private Map<String, Integer> aliasToPosition = null;
  
  public AliasableEvalFunc() {
    
  }
  
  /**
   * A wrapper method which captures the schema and then calls getOutputSchema
   */
  @Override
  public Schema outputSchema(Schema input) {
    storeFieldAliases(input);
    return getOutputSchema(input);
  }
  
  /**
   * Specify the output schema as in {link EvalFunc#outputSchema(Schema)}.
   * 
   * @param input
   * @return outputSchema
   */
  public abstract Schema getOutputSchema(Schema input);

  @SuppressWarnings("unchecked")
  private Map<String, Integer> getAliasMap() {
    return (Map<String, Integer>)getInstanceProperties().get(ALIAS_MAP_PROPERTY);
  }
  
  private void setAliasMap(Map<String, Integer> aliases) {
    getInstanceProperties().put(ALIAS_MAP_PROPERTY, aliases);
  }
  
  private void storeFieldAliases(Schema tupleSchema)
  {
    Map<String, Integer> aliases = new HashMap<String, Integer>();
    constructFieldAliases(aliases, tupleSchema, null);
    log.debug("In instance: "+getInstanceName()+", stored alias map: " + aliases);
    
    // pass the input schema into the exec function
    setAliasMap(aliases);
  }
  
  private void constructFieldAliases(Map<String, Integer> aliases, Schema tupleSchema, String prefix)
  {    
    int position = 0;
    for (Schema.FieldSchema field : tupleSchema.getFields()) {
      String alias = getPrefixedAliasName(prefix, field.alias);
      if (field.alias != null && !field.alias.equals("null")) { 
        aliases.put(alias, position);
        log.debug("In instance: "+getInstanceName()+", stored alias " + alias + " as position " + position);
      }
      if (field.schema != null) {
        constructFieldAliases(aliases, field.schema, alias);
      }      
      position++;
    }
  }
  
  public String getPrefixedAliasName(String prefix, String alias)
  {
    if (alias == null || alias.equals("null")) {
      if (prefix == null) return "";
      else return prefix; // ignore the null inner bags/tuples
    }
    else return ((prefix == null || prefix.equals("null") || prefix.trim().equals("")) ? "" : prefix+".") + alias; // handle top bag
  }
  
  /**
   * Field aliases are generated from the input schema<br/>
   * Each alias maps to a bag position<br/>
   * Inner bags/tuples will have alias of outer.inner.foo
   * 
   * @return A map of field alias to field position
   */
  public Map<String, Integer> getFieldAliases()
  {
    Map<String, Integer> aliases = getAliasMap();
    if (aliases == null) {
        log.error("Class: " + this.getClass());
        log.error("Instance name: " + this.getInstanceName());
      log.error("Properties: " + getContextProperties());
      throw new RuntimeException("Could not retrieve aliases from properties using " + ALIAS_MAP_PROPERTY);
    }
    return aliases;
  }
  
  public Integer getPosition(String alias) {
    if (aliasToPosition == null) {
      aliasToPosition = getFieldAliases();
    }
    return aliasToPosition.get(alias);
  }
  
  public Integer getPosition(String prefix, String alias) {
    return getPosition(getPrefixedAliasName(prefix, alias));
  }
      
  public Integer getInteger(Tuple tuple, String alias) throws ExecException {
    return getInteger(tuple, alias, null);
  }
  
  public Integer getInteger(Tuple tuple, String alias, Integer defaultValue) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    Number number = (Number)tuple.get(i);
    if (number == null) return defaultValue;
    return number.intValue();
  }
  
  public Long getLong(Tuple tuple, String alias) throws ExecException {
    return getLong(tuple, alias, null);
  }
  
  public Long getLong(Tuple tuple, String alias, Long defaultValue) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    Number number = (Number)tuple.get(i);
    if (number == null) return defaultValue;
    return number.longValue();
  }
  
  public Float getFloat(Tuple tuple, String alias) throws ExecException {
    return getFloat(tuple, alias, null);
  }
  
  public Float getFloat(Tuple tuple, String alias, Float defaultValue) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    Number number = (Number)tuple.get(i);
    if (number == null) return defaultValue;
    return number.floatValue();
  }
  
  public Double getDouble(Tuple tuple, String alias) throws ExecException {
    return getDouble(tuple, alias, null);
  }
  
  public Double getDouble(Tuple tuple, String alias, Double defaultValue) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    Number number = (Number)tuple.get(i);
    if (number == null) return defaultValue;
    return number.doubleValue();
  }
  
  public String getString(Tuple tuple, String alias) throws ExecException {
    return getString(tuple, alias, null);
  }
  
  public String getString(Tuple tuple, String alias, String defaultValue) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    String s = (String)tuple.get(i);
    if (s == null) return defaultValue;
    return s;
  }
  
  public Boolean getBoolean(Tuple tuple, String alias) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    return (Boolean)tuple.get(i);
  }
  
  public DataBag getBag(Tuple tuple, String alias) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    return (DataBag)tuple.get(i);
  }
  
  public Object getObject(Tuple tuple, String alias) throws ExecException {
    Integer i = getPosition(alias); 
    if (i == null) throw new FieldNotFound("Attempt to reference unknown alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    if (i >= tuple.size()) throw new FieldNotFound("Attempt to reference outside of tuple for alias: "+alias+"\n Instance Properties: "+getInstanceProperties());
    return tuple.get(i);
  }
}
