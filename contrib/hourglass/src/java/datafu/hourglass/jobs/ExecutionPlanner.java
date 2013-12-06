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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;

/**
 * Base class for execution planners.  An execution planner determines which files should be processed
 * for a particular run.
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class ExecutionPlanner
{
  private final Logger _log = Logger.getLogger(ExecutionPlanner.class);
  
  private FileSystem _fileSystem;
  private Properties _props;
  private Date _startDate;
  private Date _endDate;
  private Integer _daysAgo;
  private Integer _numDays;
  private Integer _maxToProcess;
  private DateRange _range;
  private Path _outputPath;
  private boolean _failOnMissing;
  private List<Path> _inputPaths = new ArrayList<Path>();
  private List<SortedMap<Date,DatePath>> _inputPathsByDate;
  private Map<Date,List<DatePath>> _availableInputsByDate = new HashMap<Date,List<DatePath>>();
  
  /**
   * Initializes the execution planner.
   * 
   * @param fs file system to use
   * @param props configuration properties
   */
  public ExecutionPlanner(FileSystem fs, Properties props)
  {
    _props = props;
    _fileSystem = fs;
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
   * Gets the input paths.
   * 
   * @return input paths
   */
  public List<Path> getInputPaths()
  {
    return _inputPaths;
  }

  /**
   * Sets the output path.
   * 
   * @param outputPath output path
   */
  public void setOutputPath(Path outputPath)
  {
    this._outputPath = outputPath;
  }

  /**
   * Sets the input paths.
   * 
   * @param inputPaths input paths
   */
  public void setInputPaths(List<Path> inputPaths)
  {
    this._inputPaths = inputPaths;
  }
  
  /**
   * Sets the start date.
   * 
   * @param startDate start date
   */
  public void setStartDate(Date startDate)
  {
    this._startDate = startDate;
  }
  
  /**
   * Gets the start date
   * 
   * @return start date
   */
  public Date getStartDate()
  {
    return _startDate;
  }

  /**
   * Sets the end date.
   * 
   * @param endDate end date
   */
  public void setEndDate(Date endDate)
  {
    this._endDate = endDate;
  }
  
  /**
   * Gets the end date
   * 
   * @return end date
   */
  public Date getEndDate()
  {
    return _endDate;
  }
  
  /**
   * Sets the number of days to subtract off the end date. 
   * 
   * @param daysAgo days ago
   */
  public void setDaysAgo(Integer daysAgo)
  {
    this._daysAgo = daysAgo;
  }
  
  /**
   * Gets the number of days to subtract off the end date. 
   * 
   * @return days ago
   */
  public Integer getDaysAgo()
  {
    return _daysAgo;
  }

  /**
   * Sets the number of days to process.
   * 
   * @param numDays number of days to process
   */
  public void setNumDays(Integer numDays)
  {
    this._numDays = numDays;
  }
  
  /**
   * Gets the number of days to process.
   * 
   * @return number of days to process
   */
  public Integer getNumDays()
  {
    return _numDays;
  }

  /**
   * Sets the maximum number of days to process at a time.
   * 
   * @param maxToProcess maximum number of days
   */
  public void setMaxToProcess(Integer maxToProcess)
  {
    this._maxToProcess = maxToProcess;
  }
  
  /**
   * Gets the maximum number of days to process at a time.
   * 
   * @return maximum number of days
   */
  public Integer getMaxToProcess()
  {
    return _maxToProcess;
  }
  
  /**
   * Gets whether the job should fail if data is missing within the desired date range.
   * 
   * @return true if the job should fail on missing data
   */
  public boolean isFailOnMissing()
  {
    return _failOnMissing;
  }

  /**
   * Sets whether the job should fail if data is missing within the desired date range.
   * 
   * @param failOnMissing true if the job should fail on missing data
   */
  public void setFailOnMissing(boolean failOnMissing)
  {
    this._failOnMissing = failOnMissing;
  }
  
  /**
   * Gets the desired input date range to process based on the configuration and available inputs.
   * 
   * @return desired date range
   */
  public DateRange getDateRange()
  {
    return _range;
  }

  /**
   * Gets the file system.
   * 
   * @return file system
   */
  protected FileSystem getFileSystem()
  {
    return _fileSystem;
  }
  
  /**
   * Gets the configuration properties.
   * 
   * @return properties
   */
  protected Properties getProps()
  {
    return _props;
  }
  
  /**
   * Gets a map from date to available input data.
   * 
   * @return map from date to available input data
   */
  protected Map<Date,List<DatePath>> getAvailableInputsByDate()
  {
    return _availableInputsByDate;
  }
  
  /**
   * Get a map from date to path for all paths matching yyyy/MM/dd under the given path.
   * 
   * @param path path to search under
   * @return map of date to path
   * @throws IOException
   */
  protected SortedMap<Date,DatePath> getDailyData(Path path) throws IOException
  {
    SortedMap<Date,DatePath> data = new TreeMap<Date,DatePath>();
    for (DatePath datePath : PathUtils.findNestedDatedPaths(getFileSystem(),path))
    {
      data.put(datePath.getDate(),datePath);
    }
    return data;
  }
  
  /**
   * Get a map from date to path for all paths matching yyyyMMdd under the given path.
   * 
   * @param path path to search under
   * @return map of date to path
   * @throws IOException
   */
  protected SortedMap<Date,DatePath> getDatedData(Path path) throws IOException
  {
    SortedMap<Date,DatePath> data = new TreeMap<Date,DatePath>();
    for (DatePath datePath : PathUtils.findDatedPaths(getFileSystem(),path))
    {
      data.put(datePath.getDate(),datePath);
    }
    return data;
  }
  
  /**
   * Determine what input data is available.
   * 
   * @throws IOException
   */
  protected void loadInputData() throws IOException
  {
    _inputPathsByDate = new ArrayList<SortedMap<Date,DatePath>>();
    for (Path inputPath : getInputPaths())
    {
      _log.info(String.format("Searching for available input data in " + inputPath));
      _inputPathsByDate.add(getDailyData(inputPath));
    }
  }
  
  /**
   * Determines what input data is available.
   */
  protected void determineAvailableInputDates()
  {
    // first find the latest date available for all inputs
    PriorityQueue<Date> dates = new PriorityQueue<Date>();
    for (SortedMap<Date,DatePath> pathMap : _inputPathsByDate)
    {
      for (Date date : pathMap.keySet())
      {
        dates.add(date);
      }
    }
    if (dates.size() == 0)
    {
      throw new RuntimeException("No input data!");
    }
    List<Date> available = new ArrayList<Date>();
    Date currentDate = dates.peek();
    int found = 0;
    int needed = getInputPaths().size();
    while (currentDate != null)
    {
      Date date = dates.poll();
      
      if (date != null && date.equals(currentDate))
      {
        found++;
      }
      else
      {
        if (found == needed)
        {
          available.add(currentDate);
        }
        else if (available.size() > 0)
        {
          _log.info("Did not find all input data for date " + PathUtils.datedPathFormat.format(currentDate));
          _log.info("Paths found for " + PathUtils.datedPathFormat.format(currentDate) + ":");
          // collect what's available for this date
          for (SortedMap<Date,DatePath> pathMap : _inputPathsByDate)
          {
            DatePath path = pathMap.get(currentDate);
            if (path != null)
            {
              _log.info("=> " + path);
            }
          }
          
          if (_failOnMissing)
          {
            throw new RuntimeException("Did not find all input data for date " + PathUtils.datedPathFormat.format(currentDate));
          }
          else
          {
            available.add(currentDate);
          }
        }
        
        found = 0;          
        currentDate = date;
        
        if (currentDate != null)
        {
          found++;
        }
      }        
      
      if (found > needed)
      {
        throw new RuntimeException("found more than needed");
      }        
    }
    
    _availableInputsByDate.clear();
    
    for (Date date : available)
    {
      List<DatePath> paths = new ArrayList<DatePath>();
      for (SortedMap<Date,DatePath> map : _inputPathsByDate)
      {
        DatePath path = map.get(date);
        if (path != null)
        {
          paths.add(path);
        }
      }
      _availableInputsByDate.put(date, paths);
    }
  }
  
  /**
   * Determine the date range for inputs to process based on the configuration and available inputs.
   */
  protected void determineDateRange()
  {
    _log.info("Determining range of input data to consume");
    _range = DateRangePlanner.getDateRange(getStartDate(), getEndDate(), getAvailableInputsByDate().keySet(), getDaysAgo(), getNumDays());
    if (_range.getBeginDate() == null || _range.getEndDate() == null)
    {
      throw new RuntimeException("Expected start and end date");
    }
  }
}
