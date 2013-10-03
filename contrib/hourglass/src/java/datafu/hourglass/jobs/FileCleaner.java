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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Used to remove files from the file system when they are no longer needed.
 * 
 * @author "Matthew Hayes"
 *
 */
public class FileCleaner
{
  private final Logger log = Logger.getLogger(FileCleaner.class);
  
  private final Set<Path> garbage = new HashSet<Path>();
  private final FileSystem fs;
  
  public FileCleaner(FileSystem fs)
  {
    this.fs = fs;
  }
  
  /**
   * Add a path to be removed later.
   * 
   * @param path
   * @return added path
   */
  public Path add(Path path)
  {
    garbage.add(path);
    return path;
  }
  
  /**
   * Add a path to be removed later.
   * 
   * @param path
   * @return added path
   */
  public String add(String path)
  {
    garbage.add(new Path(path));
    return path;
  }
  
  /**
   * Removes added paths from the file system.
   * 
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void clean() throws IOException
  {
    List<Path> sorted = new ArrayList<Path>(garbage);
    Collections.sort(sorted);
    for (Path p : sorted)
    {
      if (fs.exists(p))
      {
        log.info(String.format("Removing %s",p));
        fs.delete(p, true);
      }
    }
    garbage.clear();
  }
}
