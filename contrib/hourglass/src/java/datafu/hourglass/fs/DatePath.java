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

package datafu.hourglass.fs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;

/**
 * Represents a path and the corresponding date that is associated with it.
 * 
 * @author "Matthew Hayes"
 *
 */
public class DatePath implements Comparable<DatePath>
{
  private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss");
  
  private final Date date;
  private final Path path;
  
  static
  {
    timestampFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  
  public DatePath(Date date, Path path)
  {
    this.date = date;
    this.path = path;
  }
  
  public Date getDate() { return this.date; }
  public Path getPath() { return this.path; }
  
  public static DatePath createDatedPath(Path parent, Date date)
  {
    return new DatePath(date,new Path(parent,PathUtils.datedPathFormat.format(date)));
  }
  
  public static DatePath createNestedDatedPath(Path parent, Date date)
  {
    return new DatePath(date,new Path(parent,PathUtils.nestedDatedPathFormat.format(date)));
  }
  
  @Override
  public String toString()
  {
    return String.format("[date=%s, path=%s]",timestampFormat.format(this.date), this.path.toString());
  }

  @Override
  public int compareTo(DatePath o)
  {
    return this.date.compareTo(o.date);
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DatePath other = (DatePath) obj;
    if (date == null)
    {
      if (other.date != null)
        return false;
    }
    else if (!date.equals(other.date))
      return false;
    if (path == null)
    {
      if (other.path != null)
        return false;
    }
    else if (!path.equals(other.path))
      return false;
    return true;
  }
}