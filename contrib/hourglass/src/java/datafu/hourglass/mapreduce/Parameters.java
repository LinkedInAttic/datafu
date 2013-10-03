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

/**
 * Parameters used by the jobs to pass configuration settings 
 * to the mappers, combiners, and reducers.
 * 
 * @author "Matthew Hayes"
 *
 */
public class Parameters 
{
  public static final String MAPPER_IMPL_PATH = "hourglass.mapper.impl.path"; 
  public static final String REDUCER_IMPL_PATH = "hourglass.reducer.impl.path"; 
  public static final String COMBINER_IMPL_PATH = "hourglass.combiner.impl.path";
}
