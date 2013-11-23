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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Methods for working with the Hadoop distributed cache.
 * 
 * @author "Matthew Hayes"
 *
 */
public class DistributedCacheHelper 
{
  /**
   * Deserializes an object from a path in HDFS.
   * 
   * @param conf Hadoop configuration
   * @param path Path to deserialize from
   * @return Deserialized object
   * @throws IOException
   */
  public static Object readObject(Configuration conf, org.apache.hadoop.fs.Path path) throws IOException
  {
    String localPath = null;
    Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);
    for (Path localCacheFile : localCacheFiles)
    {
      if (localCacheFile.toString().endsWith(path.toString()))
      {
        localPath = localCacheFile.toString();
        break;
      }
    }
    if (localPath == null)
    {
      throw new RuntimeException("Could not find " + path + " in local cache");
    }
    FileInputStream inputStream = new FileInputStream(new File(localPath));
    ObjectInputStream objStream = new ObjectInputStream(inputStream);
    
    try
    {
      try {
        return objStream.readObject();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    finally
    {
      objStream.close();
      inputStream.close();
    }
  }
  
  /**
   * Serializes an object to a path in HDFS and adds the file to the distributed cache.
   * 
   * @param conf Hadoop configuration
   * @param obj Object to serialize
   * @param path Path to serialize object to
   * @throws IOException
   */
  public static void writeObject(Configuration conf, Object obj, org.apache.hadoop.fs.Path path) throws IOException
  {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream outputStream = fs.create(path, true);
    ObjectOutputStream objStream = new ObjectOutputStream(outputStream);
    objStream.writeObject(obj);
    objStream.close();
    outputStream.close();
    DistributedCache.addCacheFile(path.toUri(), conf);
  }
}
