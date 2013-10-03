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

import java.io.Serializable;

/**
 * Collects a sequence of values and produces one value as a result.
 * 
 * @author "Matthew Hayes"
 *
 * @param <In> Input value type
 * @param <Out> Output value type
 */
public interface Accumulator<In,Out> extends Serializable
{
  /**
   * Accumulate another value.
   *  
   * @param value Value to accumulate
   */
  void accumulate(In value);
  
  /**
   * Get the output value corresponding to all input values accumulated so far.
   * 
   * <p>
   * This may return null to indicate no record should be written.
   * </p>
   * 
   * @return Output value
   */
  Out getFinal();
  
  /**
   * Resets the internal state so that all values accumulated so far are forgotten.
   */
  void cleanup();
}
