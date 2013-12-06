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
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.hourglass.avro.AvroDateRangeMetadata;
import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;

/**
 * Execution planner used by {@link AbstractPartitionCollapsingIncrementalJob} and its derived classes.
 * This creates a plan to process partitioned input data and collapse the partitions into a single output.
 * 
 * <p>
 * To use this class, the input and output paths must be specified.  In addition the desired input date
 * range can be specified through several methods.  Then {@link #createPlan()} can be called and the
 * execution plan will be created.  The inputs to process will be available from {@link #getInputsToProcess()},
 * the number of reducers to use will be available from {@link #getNumReducers()}, and the input schemas
 * will be available from {@link #getInputSchemas()}.
 * </p>
 * 
 * <p>
 * Previous output may be reused by using {@link #setReusePreviousOutput(boolean)}.  If previous output exists
 * and it is to be reused then it will be available from {@link #getPreviousOutputToProcess()}.  New input data
 * to process that is after the previous output time range is available from {@link #getNewInputsToProcess()}.
 * Old input data to process that is before the previous output time range and should be subtracted from the
 * previous output is available from {@link #getOldInputsToProcess()}.
 * </p>
 * 
 * <p>
 * Configuration properties are used to configure a {@link ReduceEstimator} instance.  This is used to 
 * calculate how many reducers should be used.  
 * The number of reducers to use is based on the input data size and the 
 * <em>num.reducers.bytes.per.reducer</em> property.  This setting can be controlled more granularly
 * through <em>num.reducers.input.bytes.per.reducer</em> and <em>num.reducers.previous.bytes.per.reducer</em>.
 * Check {@link ReduceEstimator} for more details on how the properties are used.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitionCollapsingExecutionPlanner extends ExecutionPlanner
{
  private final Logger _log = Logger.getLogger(PartitionCollapsingExecutionPlanner.class);

  private SortedMap<Date,DatePath> _outputPathsByDate;
  private boolean _reusePreviousOutput;
  
  // the chosen execution plan
  private Plan _plan;
  
  /**
   * An execution plan.  Encapsulates what inputs will be processed.
   * 
   * @author mhayes
   *
   */
  private class Plan
  {
    private List<DatePath> _inputsToProcess = new ArrayList<DatePath>();
    private List<DatePath> _newInputsToProcess = new ArrayList<DatePath>();
    private List<DatePath> _oldInputsToProcess = new ArrayList<DatePath>();
    private Map<String,String> _latestInputByPath = new HashMap<String,String>();
    private DatePath _previousOutputToProcess;
    private List<Schema> _inputSchemas = new ArrayList<Schema>();
    private Map<String,Schema> _inputSchemasByPath = new HashMap<String,Schema>();
    private boolean _needAnotherPass;
    private DateRange _currentDateRange;
    private int _numReducers;
    private Long _totalBytes;
    
    public void finalizePlan() throws IOException
    {
      determineInputSchemas();
      determineNumReducers();
      determineTotalBytes();
    }
    
    /**
     * Determines the number of bytes that will be consumed by this execution plan.
     * This is used to compare alternative plans so the one with the least bytes
     * consumed can be used.
     * 
     * @throws IOException
     */
    private void determineTotalBytes() throws IOException
    {
      _totalBytes = 0L;
      for (DatePath dp : _inputsToProcess)
      {
        _totalBytes += PathUtils.countBytes(getFileSystem(), dp.getPath());
      }
      if (_previousOutputToProcess != null)
      {
        _totalBytes += PathUtils.countBytes(getFileSystem(), _previousOutputToProcess.getPath());
      }
      _log.info("Total bytes consumed: " + _totalBytes);
    }
    
    /**
     * Determines the input schemas.  There may be multiple input schemas because multiple inputs are allowed.
     * The latest available inputs are used to determine the schema, the assumption being that schemas are
     * backwards-compatible.
     * 
     * @throws IOException
     */
    private void determineInputSchemas() throws IOException
    {
      if (_latestInputByPath.size() > 0)
      {
        _log.info("Determining input schemas");
        for (Entry<String,String> entry : _latestInputByPath.entrySet())
        {
          String root = entry.getKey();
          String input = entry.getValue();
          _log.info("Loading schema for " + input);
          Schema schema = PathUtils.getSchemaFromPath(getFileSystem(),new Path(input));
          _inputSchemas.add(schema);
          _inputSchemasByPath.put(root, schema);
        }
      }
    }
    
    /**
     * Determines the number of reducers to use based on the input data size and the previous output,
     * if it exists and is being reused.
     * The number of reducers to use is based on the input data size and the 
     * <em>num.reducers.bytes.per.reducer</em> property.  This setting can be controlled more granularly
     * through <em>num.reducers.input.bytes.per.reducer</em> and <em>num.reducers.previous.bytes.per.reducer</em>.
     * See {@link ReduceEstimator} for details on reducer estimation.
     * 
     * @throws IOException
     */
    private void determineNumReducers() throws IOException
    {
      ReduceEstimator estimator = new ReduceEstimator(getFileSystem(),getProps());
      List<String> inputPaths = new ArrayList<String>();
      for (DatePath input : _inputsToProcess)
      {
        inputPaths.add(input.getPath().toString());
        estimator.addInputPath("input",input.getPath());
      }
      if (_previousOutputToProcess != null)
      {
        estimator.addInputPath("previous",_previousOutputToProcess.getPath());
      }
      _numReducers = estimator.getNumReducers();
    }
  }
  
  /**
   * Initializes the execution planner.
   * 
   * @param fs file system
   * @param props configuration properties
   */
  public PartitionCollapsingExecutionPlanner(FileSystem fs, Properties props)
  {
    super(fs, props);
  }

  /**
   * Create the execution plan.
   * 
   * @throws IOException
   */
  public void createPlan() throws IOException
  {
    if (_plan != null) throw new RuntimeException("Plan already exists");
    
    _log.info("Creating execution plan");
    
    loadInputData();
    loadOutputData();    
    determineAvailableInputDates();
    determineDateRange();
    
    List<Plan> plans = new ArrayList<Plan>();
    Plan plan;
    
    if (_reusePreviousOutput)
    {
      _log.info("Output may be reused, will create alternative plan that does not reuse output");
      plan = new Plan();
      try
      {
        determineInputsToProcess(false,plan);
        plan.finalizePlan();
        plans.add(plan);
      }
      catch (MaxInputDataExceededException e)
      {
        _log.info(e.getMessage());
      }
    }
    
    _log.info(String.format("Creating plan that %s previous output",(_reusePreviousOutput ? "reuses" : "does not reuse")));
    plan = new Plan();
    try
    {
      determineInputsToProcess(_reusePreviousOutput,plan);
    }
    catch (MaxInputDataExceededException e)
    {
      throw new RuntimeException(e);
    }
    plan.finalizePlan();
    plans.add(plan);
    
    if (plans.size() > 1)
    { 
      _log.info(String.format("There are %d alternative execution plans:",plans.size()));
      
      for (Plan option : plans)
      {
        _log.info(String.format("* Consume %d new inputs, %d old inputs, %s previous output (%d bytes)",
                                option._newInputsToProcess.size(),
                                option._oldInputsToProcess.size(),
                                option._previousOutputToProcess != null ? "reuse" : "no",
                                option._totalBytes));
      }
      
      // choose plan with least bytes consumed
      Collections.sort(plans, new Comparator<Plan>() {
        @Override
        public int compare(Plan o1, Plan o2)
        {
          return o1._totalBytes.compareTo(o2._totalBytes);
        }      
      });
      _plan = plans.get(0);
      
      _log.info(String.format("Choosing plan consuming %d bytes",_plan._totalBytes));
    }
    else
    {
      _plan = plans.get(0);
    }
  } 

  /**
   * Gets whether previous output should be reused, if it exists.
   * 
   * @return true if previous output should be reused
   */
  public boolean getReusePreviousOutput()
  {
    return _reusePreviousOutput;
  }
  
  /**
   * Sets whether previous output should be reused, if it exists.
   * 
   * @param reuse true if previous output should be reused
   */
  public void setReusePreviousOutput(boolean reuse)
  {
    _reusePreviousOutput = reuse;
  }
  
  /**
   * Get the number of reducers to use based on the input and previous output data size.
   * Must call {@link #createPlan()} first.
   * 
   * @return number of reducers to use
   */
  public int getNumReducers()
  {
    checkPlanExists();
    return getPlan()._numReducers;
  }
  
  public DateRange getCurrentDateRange()
  {
    checkPlanExists();
    return getPlan()._currentDateRange;
  }
  
  /**
   * Gets the previous output to reuse, or null if no output is being reused.
   * Must call {@link #createPlan()} first.
   * 
   * @return previous output to reuse, or null
   */
  public DatePath getPreviousOutputToProcess()
  {
    return getPlan()._previousOutputToProcess;
  }
  
  /**
   * Gets all inputs that will be processed.  This includes both old and new data.
   * Must call {@link #createPlan()} first.
   * 
   * @return inputs to process
   */
  public List<DatePath> getInputsToProcess()
  {
    return getPlan()._inputsToProcess;
  }
  
  /**
   * Gets only the new data that will be processed.  New data is data that falls within the 
   * desired date range.
   * Must call {@link #createPlan()} first.
   * 
   * @return new inputs to process
   */
  public List<DatePath> getNewInputsToProcess()
  {
    return getPlan()._newInputsToProcess;
  }
  
  /**
   * Gets only the old data that will be processed.  Old data is data that falls before the
   * desired date range.  It will be subtracted out from the previous output.
   * Must call {@link #createPlan()} first.
   * 
   * @return old inputs to process
   */
  public List<DatePath> getOldInputsToProcess()
  {
    return getPlan()._oldInputsToProcess;
  }
  
  /**
   * Gets whether another pass will be required.  Because there may be a limit on the number of inputs processed 
   * in a single run, multiple runs may be required to process all data in the desired date range.  
   * Must call {@link #createPlan()} first.
   * 
   * @return true if another pass is required
   */
  public boolean getNeedsAnotherPass()
  {
    return getPlan()._needAnotherPass;
  }
  
  /**
   * Gets the input schemas.  Because multiple inputs are allowed, there may be multiple schemas.
   * Must call {@link #createPlan()} first.
   * 
   * <p>
   * This does not include the output schema, even though previous output may be fed back as input.
   * The reason is that the ouput schema it determined based on the input schema.
   * </p>
   * 
   * @return input schemas
   */
  public List<Schema> getInputSchemas()
  {
    return getPlan()._inputSchemas;
  }
  
  /**
   * Gets a map from input path to schema.  Because multiple inputs are allowed, there may be multiple schemas.
   * Must call {@link #createPlan()} first.
   * 
   * @return map from path to input schema
   */
  public Map<String,Schema> getInputSchemasByPath()
  {
    return getPlan()._inputSchemasByPath;
  }
  
  /**
   * Determines what output data already exists.  Previous output may be reused.
   * 
   * @throws IOException
   */
  private void loadOutputData() throws IOException
  {
    if (getOutputPath() == null)
    {
      throw new RuntimeException("No output path specified");
    }
    _log.info(String.format("Searching for existing output data in " + getOutputPath()));
    _outputPathsByDate = getDatedData(getOutputPath());
    _log.info(String.format("Found %d output paths",_outputPathsByDate.size()));
  }
  
  /**
   * Determines what input data to process.
   * 
   * <p>
   * The input data to consume is determined by the desired date range.  If previous output is not reused then the input data to process
   * will coincide with the date range.  If previous output may be reused and previous output exists, then the input data to process 
   * will consist of new data and potentially old data.  The new input data to process is data that has time after the previous output date range,
   * so that it may be added to the previous output.
   * The old data to process is data that has time before the previous output date range, so that it may be subtracted from the previous output.
   * </p>
   * 
   * <p>
   * If there is a limit on how many days of input data can be processed then it may be the case that not all input data will be processed in
   * a single run.
   * </p>
   * 
   * @throws IOException
   * @throws MaxInputDataExceededException 
   */
  private void determineInputsToProcess(boolean reusePreviousOutput, Plan plan) throws IOException, MaxInputDataExceededException
  {
    Calendar cal = Calendar.getInstance(PathUtils.timeZone);    
        
    DateRange outputDateRange = null;
    
    if (reusePreviousOutput)
    {
      if (_outputPathsByDate.size() > 0)
      {
        DatePath latestPriorOutput = _outputPathsByDate.get(Collections.max(_outputPathsByDate.keySet()));
        _log.info("Have previous output, determining what previous incremental data to difference out");
        outputDateRange = AvroDateRangeMetadata.getOutputFileDateRange(getFileSystem(),latestPriorOutput.getPath());
        _log.info(String.format("Previous output has date range %s to %s",
                  PathUtils.datedPathFormat.format(outputDateRange.getBeginDate()),
                  PathUtils.datedPathFormat.format(outputDateRange.getEndDate())));
        
        for (Date currentDate=outputDateRange.getBeginDate(); 
             currentDate.compareTo(getDateRange().getBeginDate()) < 0
             && currentDate.compareTo(outputDateRange.getEndDate()) <= 0;)
        {
          if (!getAvailableInputsByDate().containsKey(currentDate))
          {  
            throw new RuntimeException(String.format("Missing incremental data for %s, so can't remove it from previous output",PathUtils.datedPathFormat.format(currentDate)));
          }
          
          List<DatePath> inputs = getAvailableInputsByDate().get(currentDate);
          
          for (DatePath input : inputs)
          {
            _log.info(String.format("Old Input: %s",input.getPath()));
            plan._inputsToProcess.add(input);
            plan._oldInputsToProcess.add(input);
            
            Path root = PathUtils.getNestedPathRoot(input.getPath());
            plan._latestInputByPath.put(root.toString(), input.getPath().toString());
          }
                                  
          cal.setTime(currentDate);
          cal.add(Calendar.DAY_OF_MONTH, 1);
          currentDate = cal.getTime();
        }
          
        plan._previousOutputToProcess = latestPriorOutput;
        _log.info("Previous Output: " + plan._previousOutputToProcess.getPath());
      }
      else
      {
        _log.info("No previous output to reuse");
      }
    }
    
    // consume the incremental data and produce the final output
    
    int newDataCount = 0;
    Date startDate = getDateRange().getBeginDate();
    Date endDate = startDate;
    for (Date currentDate=startDate; currentDate.compareTo(getDateRange().getEndDate()) <= 0; )
    { 
      if (getMaxToProcess() != null && newDataCount >= getMaxToProcess())
      {
        if (!reusePreviousOutput)
        {
          throw new MaxInputDataExceededException(String.format("Amount of input data has exceeded max of %d however output is not being reused so cannot do in multiple passes", getMaxToProcess()));
        }
        
        // too much data to process in a single run, will require another pass
        plan._needAnotherPass = true;
        break;
      }
      
      if (outputDateRange == null || currentDate.compareTo(outputDateRange.getEndDate()) > 0)
      {
        if (!getAvailableInputsByDate().containsKey(currentDate))
        {
          if (isFailOnMissing())
          {
            throw new RuntimeException("missing " + PathUtils.datedPathFormat.format(currentDate));            
          }
          else
          {
            _log.info("No input data found for " + PathUtils.datedPathFormat.format(currentDate));
          }
        }
        else
        {
          List<DatePath> inputs = getAvailableInputsByDate().get(currentDate);
          
          for (DatePath input : inputs)
          {
            _log.info(String.format("New Input: %s",input.getPath()));
            plan._inputsToProcess.add(input);
            plan._newInputsToProcess.add(input);
            
            Path root = PathUtils.getNestedPathRoot(input.getPath());
            plan._latestInputByPath.put(root.toString(), input.getPath().toString());
          }
                    
          newDataCount++;
        }
      }
      
      cal.setTime(currentDate);
      endDate = cal.getTime();
      cal.add(Calendar.DAY_OF_MONTH, 1);
      currentDate = cal.getTime();
    }
    
    plan._currentDateRange = new DateRange(startDate,endDate);
  } 
  
  /**
   * Throws an exception if the plan hasn't been created.
   */
  private void checkPlanExists()
  {
    if (_plan == null) throw new RuntimeException("Must call createPlan first");
  }
  
  private Plan getPlan()
  {
    checkPlanExists();
    return _plan;
  }
}
