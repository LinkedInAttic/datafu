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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.hourglass.fs.PathUtils;

/**
 * Estimates the number of reducers needed based on input size.
 * 
 * <p>
 * This sums the size of the inputs and uses bytes-per-reducer
 * settings to compute the number of reducers.  By default,
 * the bytes-per-reducer is 256 MB.  This means that if the
 * total input size is 1 GB, the total number of reducers
 * computed will be 4.
 * </p>
 * 
 * <p>
 * The bytes-per-reducer can be configured through properties
 * provided in the constructor.  The default bytes-per-reducer
 * can be overriden by setting <em>num.reducers.bytes.per.reducer</em>.
 * For example, if 536870912 (512 MB) is used for this setting,
 * then 2 reducers would be used for 1 GB.
 * </p>
 * 
 * <p>
 * The bytes-per-reducer can also be configured separately for
 * different types of inputs.  Inputs can be identified by a tag.
 * For example, if an input is tagged with <em>mydata</em>, then
 * the reducers for this input data can be configured with
 * <em>num.reducers.mydata.bytes.per.reducer</em>.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class ReduceEstimator
{
  private final Logger _log = Logger.getLogger(ReduceEstimator.class);
  
  private final Set<Path> inputPaths = new HashSet<Path>(); 
  private final Map<Path,String> pathToTag = new HashMap<Path,String>();
  private final Map<String, Long> tagToBytesPerReducer = new HashMap<String,Long>();
  private final FileSystem fs;
  
  private final static String DEFAULT = "default";
  private final static Long DEFAULT_BYTES_PER_REDUCER = 256L*1024L*1024L; // 256 MB
  
  public ReduceEstimator(FileSystem fs, Properties props)
  {
    this.fs = fs;
    
    if (props != null)
    {
      for (Object o : props.keySet())
      {
        String key = (String)o;
        if (key.startsWith("num.reducers."))
        {
          if (key.equals("num.reducers.bytes.per.reducer"))
          {
            tagToBytesPerReducer.put(DEFAULT, Long.parseLong(props.getProperty(key)));
          }
          else
          {
            Pattern p = Pattern.compile("num\\.reducers\\.([a-z]+)\\.bytes\\.per\\.reducer");
            Matcher m = p.matcher(key);
            if (m.matches())
            {
              String tag = m.group(1);
              tagToBytesPerReducer.put(tag, Long.parseLong(props.getProperty(key)));
            }
            else
            {
              throw new RuntimeException("Property not recognized: " + key);
            }
          }
        }
      }
    }
    
    if (!tagToBytesPerReducer.containsKey(DEFAULT))
    {
      long defaultValue = DEFAULT_BYTES_PER_REDUCER;
      _log.info(String.format("No default bytes per reducer set, using %.2f MB",toMB(defaultValue)));
      tagToBytesPerReducer.put(DEFAULT, defaultValue);
    }
  }
  
  public void addInputPath(Path input)
  {
    addInputPath(DEFAULT,input);
  }
  
  public void addInputPath(String tag, Path input)
  {
    if (!inputPaths.contains(input))
    {
      inputPaths.add(input);
      pathToTag.put(input, tag);
    }
    else
    {
      throw new RuntimeException("Already added input: " + input);
    }
  }
  
  public int getNumReducers() throws IOException
  {
    Map<String,Long> bytesPerTag = getTagToInputBytes();
    
    double numReducers = 0.0;
    for (String tag : bytesPerTag.keySet())
    {
      long bytes = bytesPerTag.get(tag);
      _log.info(String.format("Found %d bytes (%.2f GB) for inputs tagged with '%s'",bytes,toGB(bytes),tag));
      Long bytesPerReducer = tagToBytesPerReducer.get(tag);
      if (bytesPerReducer == null) 
      {
        bytesPerReducer = tagToBytesPerReducer.get(DEFAULT);
        
        if (bytesPerReducer == null) 
        {
          throw new RuntimeException("Could not determine bytes per reducer");
        }
        
        _log.info(String.format("No configured bytes per reducer for '%s', using default value of %.2f MB",tag,toMB(bytesPerReducer)));        
      }
      else
      {
        _log.info(String.format("Using configured bytes per reducer for '%s' of %.2f MB",tag,toMB(bytesPerReducer)));
      }
      
      double partialNumReducers = bytes/(double)bytesPerReducer;
      
      _log.info(String.format("Reducers computed for '%s' is %.2f",tag,partialNumReducers));
      
      numReducers += bytes/(double)bytesPerReducer;
    }
    
    int finalNumReducers = Math.max(1, (int)Math.ceil(numReducers));
    
    _log.info(String.format("Final computed reducers is: %d",finalNumReducers));
    
    return finalNumReducers;
  }
  
  private static double toGB(long bytes)
  {
    return bytes/(1024.0*1024.0*1024.0);
  }
  
  private static double toMB(long bytes)
  {
    return bytes/(1024.0*1024.0);
  }
  
  /**
   * Gets the total number of bytes per tag.
   * 
   * @return Map from tag to total bytes
   * @throws IOException
   */
  private Map<String,Long> getTagToInputBytes() throws IOException
  {
    Map<String,Long> result = new HashMap<String,Long>();
    for (Path input : inputPaths)
    {
      long bytes = PathUtils.countBytes(fs, input);
      String tag = pathToTag.get(input);
      if (tag == null) throw new RuntimeException("Could not find tag for input: " + input);
      Long current = result.get(tag);
      if (current == null) current = 0L;
      current += bytes;
      result.put(tag, current);
    }
    return result;
  }
}
