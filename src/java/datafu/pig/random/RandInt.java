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
 
package datafu.pig.random;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Generates a uniformly distributed integer between two bounds.
 */
@Nondeterministic
public class RandInt extends SimpleEvalFunc<Integer> 
{
  private final Random rand = new Random();
  
  /**
   * @param min lower bound for random number
   * @param max upper bound for random number
   */
  public Integer call(Integer min, Integer max) throws IOException
  {
    try
    {
      if (min > max)
      {
        throw new RuntimeException("The first argument must be less than the second");
      }
      return rand.nextInt(max - min + 1) + min;
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema(new Schema.FieldSchema("rand", DataType.INTEGER));
  }
  
}

