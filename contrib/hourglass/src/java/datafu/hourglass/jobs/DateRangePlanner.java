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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import org.apache.log4j.Logger;

import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;

/**
 * Determines the date range of inputs which should be processed.
 * 
 * @author "Matthew Hayes"
 *
 */
public class DateRangePlanner
{
  private final static Logger _log = Logger.getLogger(DateRangePlanner.class);
  
  /**
   * Determines the date range of inputs which should be processed.
   *  
   * @param beginDateOverride Begin date
   * @param endDateOverride End date
   * @param available The input dates which are available
   * @param daysAgo Number of days to subtract from the end date
   * @param numDays Number of days to process
   * @return desired date range for inputs to be processed
   */
  public static DateRange getDateRange(Date beginDateOverride, 
                                       Date endDateOverride,
                                       Collection<Date> available,
                                       Integer daysAgo,
                                       Integer numDays)
  {
    Date beginDate = null;
    Date endDate = null;
    
    Calendar cal = Calendar.getInstance(PathUtils.timeZone);
            
    // determine the range of available input data
    Date beginAvailable = null;
    Date endAvailable = null;    
    if (available != null && available.size() > 0)
    {
      ArrayList<Date> sortedAvailable = new ArrayList<Date>(available);
      Collections.sort(sortedAvailable);
      beginAvailable = sortedAvailable.get(0);
      endAvailable = sortedAvailable.get(sortedAvailable.size()-1);
    }
    else
    {
      throw new IllegalArgumentException("No data available");
    }
    
    if (endDateOverride != null && beginDateOverride != null)
    {
      beginDate = beginDateOverride;
      endDate = endDateOverride;
      
      _log.info(String.format("Specified begin date is %s",
                              PathUtils.datedPathFormat.format(beginDate)));
      _log.info(String.format("Specified end date is %s",
                              PathUtils.datedPathFormat.format(endDate)));
      
      if (daysAgo != null)
      {
        throw new IllegalArgumentException("Cannot specify days ago when begin and end date set");
      }
       
      if (numDays != null)
      {
        throw new IllegalArgumentException("Cannot specify num days when begin and end date set");
      }
    }
    else if (beginDateOverride != null)
    {
      beginDate = beginDateOverride;
      
      _log.info(String.format("Specified begin date is %s",
                              PathUtils.datedPathFormat.format(beginDate)));
      
      if (numDays != null)
      { 
        cal.setTime(beginDate);
        cal.add(Calendar.DAY_OF_MONTH, numDays-1);
        endDate = cal.getTime();
        _log.info(String.format("Num days is %d, giving end date of %s",
                                numDays, PathUtils.datedPathFormat.format(endDate)));
      }
    }
    else if (endDateOverride != null)
    {
      endDate = endDateOverride;
      
      _log.info(String.format("Specified end date is %s",
                              PathUtils.datedPathFormat.format(endDate)));
      
      if (numDays != null)
      {
        cal.setTime(endDate);
        cal.add(Calendar.DAY_OF_MONTH, -(numDays-1));
        beginDate = cal.getTime();
        _log.info(String.format("Num days is %d, giving begin date of %s",
                                numDays, PathUtils.datedPathFormat.format(beginDate)));
      }
    }
    
    if (endDate == null)
    {
      endDate = endAvailable;
            
      _log.info(String.format("No end date specified, using date for latest available input: %s",
                              PathUtils.datedPathFormat.format(endDate)));
      
      if (daysAgo != null)
      {
        cal.setTime(endDate);
        cal.add(Calendar.DAY_OF_MONTH, -daysAgo);
        endDate = cal.getTime();
        _log.info(String.format("However days ago is %d, giving end date of %s",
                                daysAgo, PathUtils.datedPathFormat.format(endDate)));
      }
    }
    
    if (endAvailable.compareTo(endDate) < 0)
    {
      throw new IllegalArgumentException(String.format("Latest available date %s is less than desired end date %s",
                                                       PathUtils.datedPathFormat.format(endAvailable),
                                                       PathUtils.datedPathFormat.format(endDate)));
    }
    
    if (beginDate == null)
    {
      beginDate = beginAvailable;
      
      if (numDays != null)
      {
        cal.setTime(endDate);
        cal.add(Calendar.DAY_OF_MONTH, -(numDays-1));
        beginDate = cal.getTime();
        _log.info(String.format("Num days is %d, giving begin date of %s",
                                numDays, PathUtils.datedPathFormat.format(beginDate)));
      }
    }
    
    if (beginAvailable.compareTo(beginDate) > 0)
    {
      throw new IllegalArgumentException(String.format("Desired begin date is %s but the next available date is %s",                                                       
                                                       PathUtils.datedPathFormat.format(beginDate),
                                                       PathUtils.datedPathFormat.format(beginAvailable)));
    }
    
    _log.info(String.format("Determined date range of inputs to consume is [%s,%s]",
                           PathUtils.datedPathFormat.format(beginDate),
                           PathUtils.datedPathFormat.format(endDate)));
    
    return new DateRange(beginDate, endDate);
  }
}
