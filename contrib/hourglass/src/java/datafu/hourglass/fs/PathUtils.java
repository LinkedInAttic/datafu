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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;


/**
 * A collection of utility methods for dealing with files and paths.
 * 
 * @author "Matthew Hayes"
 *
 */
public class PathUtils
{
  private static Logger _log = Logger.getLogger(PathUtils.class);
  
  public final static TimeZone timeZone = TimeZone.getTimeZone("UTC");    
  public static final SimpleDateFormat datedPathFormat = new SimpleDateFormat("yyyyMMdd");  
  public static final SimpleDateFormat nestedDatedPathFormat = new SimpleDateFormat("yyyy/MM/dd");  
  private static final Pattern timestampPathPattern = Pattern.compile(".+/(\\d{8})");
  private static final Pattern dailyPathPattern = Pattern.compile("(.+)/(\\d{4}/\\d{2}/\\d{2})");

  /**
   * Filters out paths starting with "." and "_".
   */
  public static final PathFilter nonHiddenPathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path)
    {
      String s = path.getName().toString();
      return !s.startsWith(".") && !s.startsWith("_");        
    }
  };
  
  static
  {
    datedPathFormat.setTimeZone(timeZone);
    nestedDatedPathFormat.setTimeZone(timeZone);
  }
  
  /**
   * Delete all but the last N days of paths matching the "yyyyMMdd" format.
   * 
   * @param fs
   * @param path
   * @param retentionCount
   * @throws IOException
   */
  public static void keepLatestDatedPaths(FileSystem fs, Path path, int retentionCount) throws IOException
  {
    LinkedList<DatePath> outputs = new LinkedList<DatePath>(PathUtils.findDatedPaths(fs, path));
    
    while (outputs.size() > retentionCount)
    {
      DatePath toDelete = outputs.removeFirst();
      _log.info(String.format("Removing %s",toDelete.getPath()));
      fs.delete(toDelete.getPath(),true);
    }
  }
  
  /**
   * Delete all but the last N days of paths matching the "yyyy/MM/dd" format.
   * 
   * @param fs
   * @param path
   * @param retentionCount
   * @throws IOException
   */
  public static void keepLatestNestedDatedPaths(FileSystem fs, Path path, int retentionCount) throws IOException
  {
    List<DatePath> outputPaths = PathUtils.findNestedDatedPaths(fs, path);
    
    if (outputPaths.size() > retentionCount)
    {
      Collections.sort(outputPaths);
      _log.info(String.format("Applying retention range policy"));
      for (DatePath output : outputPaths.subList(0, outputPaths.size() - retentionCount))
      {
        _log.info(String.format("Removing %s because it is outside retention range",output.getPath()));
        fs.delete(output.getPath(),true);
      }
    }
  }
  
  /**
   * List all paths matching the "yyyy/MM/dd" format under a given path.
   * 
   * @param fs file system
   * @param input path to search under
   * @return paths
   * @throws IOException
   */
  public static List<DatePath> findNestedDatedPaths(FileSystem fs, Path input) throws IOException
  {
    List<DatePath> inputDates = new ArrayList<DatePath>();
    
    FileStatus[] pathsStatus = fs.globStatus(new Path(input,"*/*/*"), nonHiddenPathFilter);
    
    if (pathsStatus == null)
    {
      return inputDates;
    }
        
    for (FileStatus pathStatus : pathsStatus)
    {
      Matcher matcher = dailyPathPattern.matcher(pathStatus.getPath().toString());
      if (matcher.matches())
      {
        String datePath = matcher.group(2);
        Date date;
        try
        {
          date = nestedDatedPathFormat.parse(datePath);
        }
        catch (ParseException e)
        {
          continue;
        }
        
        Calendar cal = Calendar.getInstance(timeZone);
        
        cal.setTimeInMillis(date.getTime());
        
        inputDates.add(new DatePath(cal.getTime(), pathStatus.getPath()));
      }
    }
    
    return inputDates;
  }
  
  /**
   * List all paths matching the "yyyyMMdd" format under a given path.
   * 
   * @param fs file system
   * @param path path to search under
   * @return paths
   * @throws IOException
   */
  public static List<DatePath> findDatedPaths(FileSystem fs, Path path) throws IOException
  {
    FileStatus[] outputPaths = fs.listStatus(path, nonHiddenPathFilter);
    
    List<DatePath> outputs = new ArrayList<DatePath>();
    
    if (outputPaths != null)
    {
      for (FileStatus outputPath : outputPaths)
      {
        Date date;
        try
        {
          date = datedPathFormat.parse(outputPath.getPath().getName());
        }
        catch (ParseException e)
        {
          continue;
        }
        
        outputs.add(new DatePath(date,outputPath.getPath()));
      }
    }
    
    Collections.sort(outputs);
    
    return outputs;
  }
  
  /**
   * Gets the schema from a given Avro data file.
   * 
   * @param fs 
   * @param path
   * @return The schema read from the data file's metadata.
   * @throws IOException
   */
  public static Schema getSchemaFromFile(FileSystem fs, Path path) throws IOException
  {
    FSDataInputStream dataInputStream = fs.open(path);
    DatumReader <GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(dataInputStream, reader);
    try
    {
      return dataFileStream.getSchema();
    }
    finally
    {
      dataFileStream.close();
    }
  }
  
  /**
   * Gets the schema for the first Avro file under the given path.
   * 
   * @param path path to fetch schema for
   * @return Avro schema
   * @throws IOException
   */
  public static Schema getSchemaFromPath(FileSystem fs, Path path) throws IOException
  {
    return getSchemaFromFile(fs,fs.listStatus(path, nonHiddenPathFilter)[0].getPath());
  }
  
  /**
   * Sums the size of all files listed under a given path. 
   * 
   * @param fs file system
   * @param path path to count bytes for
   * @return total bytes under path
   * @throws IOException
   */
  public static long countBytes(FileSystem fs, Path path) throws IOException
  {
    FileStatus[] files = fs.listStatus(path, nonHiddenPathFilter);
    long totalForPath = 0L;
    for (FileStatus file : files)
    {
      totalForPath += file.getLen();
    }    
    return totalForPath;
  }
  
  /**
   * Gets the date for a path in the "yyyyMMdd" format.
   * 
   * @param path path to check
   * @return date for path
   */
  public static Date getDateForDatedPath(Path path)
  {
    Matcher matcher = timestampPathPattern.matcher(path.toString());
    
    if (!matcher.matches())
    {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
         
    try
    {
      return PathUtils.datedPathFormat.parse(matcher.group(1));
    }
    catch (ParseException e)
    {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
  }
  
  /**
   * Gets the date for a path in the "yyyy/MM/dd" format.
   * 
   * @param path path to check
   * @return date
   */
  public static Date getDateForNestedDatedPath(Path path)
  {
    Matcher matcher = dailyPathPattern.matcher(path.toString());
    
    if (!matcher.matches())
    {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
    
    try
    {
      return PathUtils.nestedDatedPathFormat.parse(matcher.group(2));
    }
    catch (ParseException e)
    {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
  }
  
  /**
   * Gets the root path for a path in the "yyyy/MM/dd" format.  This is part of the path preceding the
   * "yyyy/MM/dd" portion.
   * 
   * @param path
   * @return
   */
  public static Path getNestedPathRoot(Path path)
  {
    Matcher matcher = dailyPathPattern.matcher(path.toString());
    
    if (!matcher.matches())
    {
      throw new RuntimeException("Unexpected input filename: " + path);
    }
    
    return new Path(matcher.group(1));
  }
}
