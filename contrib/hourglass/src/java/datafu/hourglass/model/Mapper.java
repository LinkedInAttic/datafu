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

package datafu.hourglass.model;

import java.io.IOException;
import java.io.Serializable;

/**
 * Maps an input record to one or more key-value pairs.
 * 
 * @author "Matthew Hayes"
 *
 * @param <In> Input type
 * @param <OutKey> Output key type
 * @param <OutVal> Output value type
 */
public interface Mapper<In,OutKey,OutVal> extends Serializable
{
  /**
   * Maps an input record to one or more key-value pairs.
   * 
   * @param input Input value
   * @param collector Collects output key-value pairs
   * @throws IOException
   * @throws InterruptedException
   */
  void map(In input, KeyValueCollector<OutKey,OutVal> collector)  throws IOException, InterruptedException;
}
