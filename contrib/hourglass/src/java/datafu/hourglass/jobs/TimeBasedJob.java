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

import java.text.ParseException;
import java.util.Date;
import java.util.Properties;

import datafu.hourglass.fs.PathUtils;

/**
 * Base class for Hadoop jobs that consume time-partitioned data.
 * 
 * <p>
 * This class has the same configuration and methods as {@link AbstractJob}.
 * In addition it also recognizes the following properties:
 * </p>
 * 
 * <ul>
 *   <li><em>num.days</em> - Number of consecutive days of input data to consume</li>
 *   <li><em>days.ago</em> - Number of days to subtract off the end of the consumption window</li>
 *   <li><em>start.date</em> - Start date for window in yyyyMMdd format</li>
 *   <li><em>end.date</em> - End date for window in yyyyMMdd format</li>
 * </ul>
 * 
 * <p>
 * Methods are available as well for setting these configuration parameters.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class TimeBasedJob extends AbstractJob
{ 
  private Integer _numDays;
  private Integer _daysAgo;
  private Date _startDate;
  private Date _endDate;  
  
  /**
   * Initializes the job.
   */
  public TimeBasedJob()
  {    
  }
  
  /**
   * Initializes the job with a job name and properties.
   * 
   * @param name Job name
   * @param props Configuration properties
   */
  public TimeBasedJob(String name, Properties props)
  {        
    super(name,props);
  }
  
  @Override
  public void setProperties(Properties props)
  {
    super.setProperties(props);
    
    if (getProperties().get("num.days") != null)
    {
      setNumDays(Integer.parseInt((String)getProperties().get("num.days")));
    }
    
    if (getProperties().get("days.ago") != null)
    {
      setDaysAgo(Integer.parseInt((String)getProperties().get("days.ago")));
    }
    
    if (getProperties().get("start.date") != null)
    {
      try
      {
        // start date treated as inclusive lower bound
        setStartDate(PathUtils.datedPathFormat.parse((String)getProperties().get("start.date")));
      }
      catch (ParseException e)
      {
        throw new IllegalArgumentException(e);
      }
    }
    
    if (getProperties().get("end.date") != null)
    {
      try
      {
        setEndDate(PathUtils.datedPathFormat.parse((String)getProperties().get("end.date")));
      }
      catch (ParseException e)
      {
        throw new IllegalArgumentException(e);
      }
    }
  }
  
  /**
   * Gets the number of consecutive days to process.
   * 
   * @return number of days to process
   */
  public Integer getNumDays()
  {
    return _numDays;
  }

  /**
   * Sets the number of consecutive days to process.
   * 
   * @param numDays number of days to process
   */
  public void setNumDays(Integer numDays)
  {
    this._numDays = numDays;
  }

  /**
   * Gets the number of days to subtract off the end of the consumption window.
   * 
   * @return Days ago
   */
  public Integer getDaysAgo()
  {
    return _daysAgo;
  }

  /**
   * Sets the number of days to subtract off the end of the consumption window.
   * 
   * @param daysAgo Days ago
   */
  public void setDaysAgo(Integer daysAgo)
  {
    this._daysAgo = daysAgo;
  }

  /**
   * Gets the start date.
   * 
   * @return Start date
   */
  public Date getStartDate()
  {
    return _startDate;
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
   * Gets the end date.
   * 
   * @return end date
   */
  public Date getEndDate()
  {
    return _endDate;
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
  
  @Override
  protected void validate()
  {
    super.validate();
    
    if (_daysAgo != null && _endDate != null)
    {
      throw new IllegalArgumentException("Cannot specify both end date and days ago");
    }
    
    if (_numDays != null && _startDate != null && _endDate != null)
    {
      throw new IllegalArgumentException("Cannot specify num days when both start date and end date are set");
    }
  }
}
