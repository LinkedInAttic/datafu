/**
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

package datafu.hourglass.mapreduce;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper which outputs key-value pairs as-is.
 * 
 * It assumes the input is an Avro record having "key" and "value" fields.
 * The output is these exact same fields.
 * 
 * @author "Matthew Hayes"
 *
 */
public class AvroKeyValueIdentityMapper extends Mapper<Object, Object, Object, Object> 
{
  @Override
  protected void map(Object keyObj, Object valueObj, Context context) throws java.io.IOException, java.lang.InterruptedException
  {
    @SuppressWarnings("unchecked")
    GenericRecord input = ((AvroKey<GenericRecord>)keyObj).datum();
    GenericRecord key = (GenericRecord)input.get("key");
    GenericRecord value = (GenericRecord)input.get("value");
    context.write(new AvroKey<GenericRecord>(key),new AvroValue<GenericRecord>(value));
  }
}
