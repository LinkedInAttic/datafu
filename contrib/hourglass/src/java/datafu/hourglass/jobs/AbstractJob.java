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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Base class for Hadoop jobs.
 * 
 * <p>
 * This class defines a set of common methods and configuration shared by Hadoop jobs.
 * Jobs can be configured either by providing properties or by calling setters.
 * Each property has a corresponding setter.
 * </p>
 * 
 * This class recognizes the following properties:
 * 
 * <ul>
 *   <li><em>input.path</em> - Input path job will read from</li>
 *   <li><em>output.path</em> - Output path job will write to</li>
 *   <li><em>temp.path</em> - Temporary path under which intermediate files are stored</li>
 *   <li><em>retention.count</em> - Number of days to retain in output directory</li>
 *   <li><em>num.reducers</em> - Number of reducers to use</li>
 *   <li><em>use.combiner</em> - Whether to use a combiner or not</li>
 *   <li><em>counters.path</em> - Path to store job counters in</li>
 * </ul>
 * 
 * <p>
 * The <em>input.path</em> property may be a comma-separated list of paths.  When there is more
 * than one it implies a join is to be performed.  Alternatively the paths may be listed separately.
 * For example, <em>input.path.first</em> and <em>input.path.second</em> define two separate input
 * paths.
 * </p>
 * 
 * <p>
 * The <em>num.reducers</em> fixes the number of reducers.  When not set the number of reducers
 * is computed based on the input size.
 * </p>
 * 
 * <p>
 * The <em>temp.path</em> property defines the parent directory for temporary paths, not the
 * temporary path itself.  Temporary paths are created under this directory with an <em>hourglass-</em>
 * prefix followed by a GUID.
 * </p>
 * 
 * <p> 
 * The input and output paths are the only required parameters.  The rest are optional.
 * </p>
 * 
 * <p>
 * Hadoop configuration may be provided by setting a property with the prefix <em>hadoop-conf.</em>.
 * For example, <em>mapred.min.split.size</em> can be configured by setting property
 * <em>hadoop-conf.mapred.min.split.size</em> to the desired value. 
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class AbstractJob extends Configured
{
  private static String HADOOP_PREFIX = "hadoop-conf.";
  
  private Properties _props;
  private String _name;
  private boolean _useCombiner;
  private Path _countersParentPath;
  private Integer _numReducers;
  private Integer _retentionCount;
  private List<Path> _inputPaths;
  private Path _outputPath;
  private Path _tempPath = new Path("/tmp");
  private FileSystem _fs;
  
  /**
   * Initializes the job.
   */
  public AbstractJob()
  {    
    setConf(new Configuration());
  }
    
  /**
   * Initializes the job with a job name and properties.
   * 
   * @param name Job name
   * @param props Configuration properties
   */
  public AbstractJob(String name, Properties props)
  {        
    this();
    setName(name);
    setProperties(props);
  }
  
  /**
   * Gets the job name
   * 
   * @return Job name
   */
  public String getName()
  {
    return _name;
  }
  
  /**
   * Sets the job name
   * 
   * @param name Job name
   */
  public void setName(String name)
  {
    _name = name;
  }
  
  /**
   * Gets the configuration properties.
   * 
   * @return Configuration properties
   */
  public Properties getProperties()
  {
    return _props;
  }
  
  /**
   * Sets the configuration properties.
   * 
   * @param props Properties
   */
  public void setProperties(Properties props)
  {
    _props = props;
    updateConfigurationFromProps(_props);
    
    if (_props.get("input.path") != null) 
    {
      String[] pathSplit = ((String)_props.get("input.path")).split(",");
      List<Path> paths = new ArrayList<Path>();
      for (String path : pathSplit)
      {
        if (path != null && path.length() > 0)
        {
          path = path.trim();
          if (path.length() > 0)
          {
            paths.add(new Path(path));
          }
        }
      }
      if (paths.size() > 0)
      {
        setInputPaths(paths);
      }
      else
      {
        throw new RuntimeException("Could not extract input paths from: " + _props.get("input.path"));
      }
    }
    else
    {
      List<Path> inputPaths = new ArrayList<Path>();
      for (Object o : _props.keySet())
      {
        String prop = o.toString();
        if (prop.startsWith("input.path."))
        {
          inputPaths.add(new Path(_props.getProperty(prop)));
        }
      }
      if (inputPaths.size() > 0)
      {
        setInputPaths(inputPaths);
      }
    }
    
    if (_props.get("output.path") != null) 
    {
      setOutputPath(new Path((String)_props.get("output.path")));
    }
    
    if (_props.get("temp.path") != null) 
    {
      setTempPath(new Path((String)_props.get("temp.path")));
    }
    
    if (_props.get("retention.count") != null)
    {
      setRetentionCount(Integer.parseInt((String)_props.get("retention.count")));
    }
    
    if (_props.get("num.reducers") != null)
    {
      setNumReducers(Integer.parseInt((String)_props.get("num.reducers")));
    }
    
    if (_props.get("use.combiner") != null)
    {
      setUseCombiner(Boolean.parseBoolean((String)_props.get("use.combiner")));
    }   
    
    if (_props.get("counters.path") != null)
    {
      setCountersParentPath(new Path((String)_props.get("counters.path")));
    }
  }
  
  /**
   * Overridden to provide custom configuration before the job starts.
   * 
   * @param conf
   */
  public void config(Configuration conf)
  {    
  }
  
  /**
   * Gets the number of reducers to use.
   * 
   * @return Number of reducers
   */
  public Integer getNumReducers()
  {
    return _numReducers;
  }

  /**
   * Sets the number of reducers to use.  Can also be set with <em>num.reducers</em> property.
   * 
   * @param numReducers Number of reducers to use
   */
  public void setNumReducers(Integer numReducers)
  {
    this._numReducers = numReducers;
  }
  
  /**
   * Gets whether the combiner should be used.
   * 
   * @return True if combiner should be used, otherwise false.
   */
  public boolean isUseCombiner()
  {
    return _useCombiner;
  }

  /**
   * Sets whether the combiner should be used.  Can also be set with <em>use.combiner</em>.
   * 
   * @param useCombiner True if a combiner should be used, otherwise false.
   */
  public void setUseCombiner(boolean useCombiner)
  {
    this._useCombiner = useCombiner;
  }

  /**
   * Gets the path where counters will be stored.
   * 
   * @return Counters path
   */
  public Path getCountersParentPath()
  {
    return _countersParentPath;
  }

  /**
   * Sets the path where counters will be stored.  Can also be set with <em>counters.path</em>.
   * 
   * @param countersParentPath Counters path
   */
  public void setCountersParentPath(Path countersParentPath)
  {
    this._countersParentPath = countersParentPath;
  }

  /**
   * Gets the number of days of data which will be retained in the output path.
   * Only the latest will be kept.  Older paths will be removed.
   * 
   * @return retention count
   */
  public Integer getRetentionCount()
  {
    return _retentionCount;
  }

  /**
   * Sets the number of days of data which will be retained in the output path.
   * Only the latest will be kept.  Older paths will be removed.
   * Can also be set with <em>retention.count</em>.
   * 
   * @param retentionCount
   */
  public void setRetentionCount(Integer retentionCount)
  {
    this._retentionCount = retentionCount;
  }
  
  /**
   * Gets the input paths.  Multiple input paths imply a join is to be performed.
   * 
   * @return input paths
   */
  public List<Path> getInputPaths()
  {
    return _inputPaths;
  }

  /**
   * Sets the input paths.  Multiple input paths imply a join is to be performed.
   * Can also be set with <em>input.path</em> or several properties starting with
   * <em>input.path.</em>.
   * 
   * @param inputPaths input paths
   */
  public void setInputPaths(List<Path> inputPaths)
  {
    this._inputPaths = inputPaths;
  }

  /**
   * Gets the output path.
   * 
   * @return output path
   */
  public Path getOutputPath()
  {
    return _outputPath;
  }

  /**
   * Sets the output path.  Can also be set with <em>output.path</em>.
   * 
   * @param outputPath output path
   */
  public void setOutputPath(Path outputPath)
  {
    this._outputPath = outputPath;
  }
  
  /**
   * Gets the temporary path under which intermediate files will be stored.  Defaults to /tmp.
   * 
   * @return Temporary path
   */
  public Path getTempPath()
  {
    return _tempPath;
  }

  /**
   * Sets the temporary path where intermediate files will be stored.  Defaults to /tmp. 
   * 
   * @param tempPath Temporary path
   */
  public void setTempPath(Path tempPath)
  {
    this._tempPath = tempPath;
  }
  
  /**
   * Gets the file system.
   * 
   * @return File system
   * @throws IOException 
   */
  protected FileSystem getFileSystem()
  {
    if (_fs == null)
    {
      try
      {
        _fs = FileSystem.get(getConf());
      }
      catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
    return _fs;
  }
  
  /**
   * Generates a random temporary path within the file system.  This does not create the path.
   * 
   * @return Random temporary path
   */
  protected Path randomTempPath()
  {
    return new Path(_tempPath,String.format("hourglass-%s",UUID.randomUUID()));
  }
  
  /**
   * Creates a random temporary path within the file system.
   * 
   * @return Random temporary path
   * @throws IOException
   */
  protected Path createRandomTempPath() throws IOException
  {
    return ensurePath(randomTempPath());
  }
  
  /**
   * Creates a path, if it does not already exist.
   * 
   * @param path Path to create
   * @return The same path that was provided
   * @throws IOException
   */
  protected Path ensurePath(Path path) throws IOException
  {
    if (!getFileSystem().exists(path))
    {
      getFileSystem().mkdirs(path);
    }
    return path;
  }
  
  /**
   * Validation required before running job.
   */
  protected void validate()
  {
    if (_inputPaths == null || _inputPaths.size() == 0) 
    {
      throw new IllegalArgumentException("Input path is not specified.");
    }
    
    if (_outputPath == null) 
    {
      throw new IllegalArgumentException("Output path is not specified.");
    }
  }
  
  /**
   * Initialization required before running job.
   */
  protected void initialize()
  {
  }
  
  /**
   * Run the job.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public abstract void run() throws IOException, InterruptedException, ClassNotFoundException;
  
  /**
   * Creates Hadoop configuration using the provided properties.
   * 
   * @param props
   * @return
   */
  private void updateConfigurationFromProps(Properties props)
  {
    Configuration config = getConf();
    
    if (config == null)
    {
      config = new Configuration();
    }
    
    // to enable unit tests to inject configuration  
    if (props.containsKey("test.conf"))
    {
      try
      {
        byte[] decoded = Base64.decodeBase64(props.getProperty("test.conf"));        
        ByteArrayInputStream byteInput = new ByteArrayInputStream(decoded);
        DataInputStream inputStream = new DataInputStream(byteInput);
        config.readFields(inputStream);
      }
      catch (IOException e)
      {
        throw new RuntimeException(e);
      }
    }
    else
    {
      for (String key : props.stringPropertyNames()) 
      {               
          String newKey = key;
          String value = props.getProperty(key);
          
          if (key.toLowerCase().startsWith(HADOOP_PREFIX)) {
            newKey = key.substring(HADOOP_PREFIX.length());
            config.set(newKey, value);
          }
      }      
    }
  }   
}
