/*
 * Copyright 2010 LinkedIn Corp. and contributors
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
 
package datafu.pig.sets;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Base class for set operations.
 * 
 * @author "Matthew Hayes <mhayes@linkedin.com>"
 *
 */
public abstract class SetOperationsBase extends EvalFunc<DataBag>
{
  @Override
  public Schema outputSchema(Schema input)
  {
    try {      
      for (Schema.FieldSchema fieldSchema : input.getFields())
      {
        if (fieldSchema.type != DataType.BAG)
        {
          throw new RuntimeException("Expected a bag but got: " + DataType.findTypeName(fieldSchema.type));
        }
      }
      
      Schema bagSchema = input.getField(0).schema;
                  
      Schema outputSchema = new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                             .getName()
                                                             .toLowerCase(), input),
                                           bagSchema,
                                           DataType.BAG));
            
      return outputSchema;
    }
    catch (Exception e) {
      return null;
    }
  }
}
