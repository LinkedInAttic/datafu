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

/**
 * Provided to an instance of {@link Mapper} to collect key-value pairs.
 * 
 * @author "Matthew Hayes"
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface KeyValueCollector<K,V>
{
  /**
   * Collects key-value pairs.
   * 
   * @param key Key to be collected
   * @param value Value to be collected
   * @throws IOException
   * @throws InterruptedException
   */
  void collect(K key,V value)  throws IOException, InterruptedException;
}
