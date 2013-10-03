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

import java.io.IOException;

import org.apache.hadoop.mapreduce.ReduceContext;

/**
 * Defines the interface for combiner and reducer implementations that {@link DelegatingCombiner} and
 * {@link DelegatingReducer} delegate to.
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class ObjectReducer extends ObjectProcessor {
  public abstract void reduce(Object key,
      Iterable<Object> values,
      ReduceContext<Object,Object,Object,Object> context) throws IOException,InterruptedException;
}