/*
 * Copyright 2013 LinkedIn, Inc
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
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

/**
 * Returns the first non-null value from a tuple, just like {@link <a href="http://msdn.microsoft.com/en-us/library/ms190349.aspx" target="_blank">COALESCE</a>} in SQL. 
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 *
 * define COALESCE datafu.pig.util.COALESCE();

 * -- input: 1,2,3,NULL,4,NULL,5
 * input = LOAD 'input' AS (val:int);
 *
 * -- produces: 1,2,3,99,4,99,5
 * coalesced = FOREACH input GENERATE COALESCE(val,99);
 *
 * }
 * </pre>
 * </p>
 * 
 * @author "Matthew Hayes <mhayes@linkedin.com>"
 *
 */
public class Coalesce extends EvalFunc<Object>
{
  private String instanceName;
  private boolean strict;
  
  private static String STRICT_OPTION = "strict";
  private static String LAZY_OPTION = "lazy";
  
  public Coalesce()
  {    
    strict = true;
  }
  
  public Coalesce(String option)
  {
    if (option.equals(STRICT_OPTION))
    {
      strict = true;
    }
    else if (option.equals(LAZY_OPTION))
    {
      strict = false;
    }
    else
    {
      throw new IllegalArgumentException("Unexpected option: " + option);
    }
  }
  
  @Override
  public Object exec(Tuple input) throws IOException
  {    
    if (input == null || input.size() == 0)
    {
      return null;
    }
    
    Byte type = (Byte)getInstanceProperties().get("type");
            
    for (Object o : input)
    {
      if (o != null)
      {
        if (strict)
        {
          return o;
        }
        else
        {
          switch (type)
          {
          case DataType.INTEGER:
            return DataType.toInteger(o);
          case DataType.LONG:
            return DataType.toLong(o);
          case DataType.DOUBLE:
            return DataType.toDouble(o); 
          case DataType.FLOAT:
            return DataType.toFloat(o);      
          default:
            return o;
          }
        }
      }
    }
    
    return null;
  }
  
  @Override
  public Schema outputSchema(Schema input)
  {
    if (input.getFields().size() == 0)
    {
      throw new RuntimeException("Expected at least one parameter");
    }
        
    Byte outputType = null;
    int pos = 0;
    for (FieldSchema field : input.getFields())
    {
      if (DataType.isSchemaType(field.type))
      {
        throw new RuntimeException(String.format("Not supported on schema types.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (DataType.isComplex(field.type))
      {
        throw new RuntimeException(String.format("Not supported on complex types.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (!DataType.isUsableType(field.type))
      {
        throw new RuntimeException(String.format("Not a usable type.  Found %s in position %d.",DataType.findTypeName(field.type),pos));
      }
      
      if (outputType == null)
      {
        outputType = field.type;
      }
      else if (!outputType.equals(field.type))
      {        
        if (strict)
        {
          throw new RuntimeException(String.format("Expected all types to be equal, but found '%s' in position %d.  First element has type '%s'.  "
                                                   + "If you'd like to attempt merging types, use the '%s' option, as '%s' is the default.",
                                                   DataType.findTypeName(field.type),pos,DataType.findTypeName((byte)outputType),LAZY_OPTION,STRICT_OPTION));
        }
        else
        {
          byte merged = DataType.mergeType(outputType, field.type);
          if (merged == DataType.ERROR)
          {
            throw new RuntimeException(String.format("Expected all types to be equal, but found '%s' in position %d, where output type is '%s', and types could not be merged.",
                                                     DataType.findTypeName(field.type),pos,DataType.findTypeName((byte)outputType)));
          }
          outputType = merged;
        }
      }
      
      pos++;
    }
    
    getInstanceProperties().put("type", outputType);
        
    return new Schema(new Schema.FieldSchema("item",outputType));
  }
  
  /**
   * Helper method to return the context properties for this class
   * 
   * @return context properties
   */
  protected Properties getContextProperties() {
    UDFContext context = UDFContext.getUDFContext();
    Properties properties = context.getUDFProperties(this.getClass());
    return properties;
  }
  
  /**
   * Helper method to return the context properties for this instance of this class
   * 
   * @return instances properties
   */
  protected Properties getInstanceProperties() {
    Properties contextProperties = getContextProperties();
    if (!contextProperties.containsKey(getInstanceName())) {
      contextProperties.put(getInstanceName(), new Properties());
    }
    return (Properties)contextProperties.get(getInstanceName());
  }
  
  /**
   * 
   * @return the name of this instance corresponding to the UDF Context Signature
   * @see #setUDFContextSignature(String)
   */
  protected String getInstanceName() {
    if (instanceName == null) {
      throw new RuntimeException("Instance name is null.  This should not happen unless UDFContextSignature was not set.");
    }
    return instanceName;
  }  
  
  private void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }
  
  @Override
  public void setUDFContextSignature(String signature) {
    setInstanceName(signature);
  }
}
