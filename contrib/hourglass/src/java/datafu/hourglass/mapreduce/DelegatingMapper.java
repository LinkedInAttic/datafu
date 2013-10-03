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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A Hadoop mapper which delegates to an implementation read from the distributed cache.
 * 
 * @author "Matthew Hayes"
 *
 */
public class DelegatingMapper extends Mapper<Object, Object, Object, Object> 
{
  private ObjectMapper processor;
  
  @Override
  protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
  {
    super.setup(context);
    
    if (context != null)
    {
      Configuration conf = context.getConfiguration();
      
      String path = conf.get(Parameters.MAPPER_IMPL_PATH);
      
      this.processor = (ObjectMapper)DistributedCacheHelper.readObject(conf, new Path(path));
      this.processor.setContext(context);
    }
  }
  
  @Override
  protected void map(Object key, Object value, Context context) throws java.io.IOException, java.lang.InterruptedException
  {
    this.processor.map(key, context);
  }

  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException
  {
    this.processor.close();
  }
}
