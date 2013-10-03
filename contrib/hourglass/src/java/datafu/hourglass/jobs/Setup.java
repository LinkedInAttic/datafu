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

package datafu.hourglass.jobs;

import org.apache.hadoop.conf.Configuration;

/**
 * Used as a callback by {@link PartitionCollapsingIncrementalJob} and {@link PartitionPreservingIncrementalJob}
 * to provide configuration settings for the Hadoop job.
 * 
 * @author "Matthew Hayes"
 *
 */
public interface Setup
{
  /**
   * Set custom configuration.
   * 
   * @param conf configuration
   */
  void setup(Configuration conf);
}
