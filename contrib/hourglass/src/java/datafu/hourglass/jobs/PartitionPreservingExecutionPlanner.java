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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.PathUtils;

/**
 * Execution planner used by {@link AbstractPartitionPreservingIncrementalJob} and its derived classes.
 * This creates a plan to process partitioned input data and produce partitioned output data.
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
 * Configuration properties are used to configure a {@link ReduceEstimator} instance.  This is used to 
 * calculate how many reducers should be used.  
 * The number of reducers to use is based on the input data size and the 
 * <em>num.reducers.bytes.per.reducer</em> property.
 * Check {@link ReduceEstimator} for more details on how the properties are used.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public class PartitionPreservingExecutionPlanner extends ExecutionPlanner
{
  private final Logger _log = Logger.getLogger(PartitionPreservingExecutionPlanner.class);
  
  private SortedMap<Date,DatePath> _outputPathsByDate;
  private Map<String,String> _latestInputByPath = new HashMap<String,String>();
  private List<DatePath> _inputsToProcess = new ArrayList<DatePath>();
  private List<Schema> _inputSchemas = new ArrayList<Schema>();
  private Map<String,Schema> _inputSchemasByPath = new HashMap<String,Schema>();
  private boolean _needAnotherPass;
  private int _numReducers;
  private boolean _planExists;
  
  /**
   * Initializes the execution planner.
   * 
   * @param fs file system
   * @param props configuration properties
   */
  public PartitionPreservingExecutionPlanner(FileSystem fs, Properties props)
  {
    super(fs,props);
  }

  /**
   * Create the execution plan.
   * 
   * @throws IOException
   */
  public void createPlan() throws IOException
  {
    if (_planExists) throw new RuntimeException("Plan already exists");
    _planExists = true;
    loadInputData();
    loadOutputData();
    determineAvailableInputDates();
    determineDateRange();
    determineInputsToProcess();
    determineInputSchemas();
    determineNumReducers();
  }
  
  /**
   * Get the number of reducers to use based on the input data size.
   * Must call {@link #createPlan()} first.
   * 
   * @return number of reducers to use
   */
  public int getNumReducers()
  {
    checkPlanExists();
    return _numReducers;
  }
  
  /**
   * Gets the input schemas.  Because multiple inputs are allowed, there may be multiple schemas.
   * Must call {@link #createPlan()} first.
   * 
   * @return input schemas
   */
  public List<Schema> getInputSchemas()
  {
    checkPlanExists();
    return _inputSchemas;
  }
  
  /**
   * Gets a map from input path to schema.  Because multiple inputs are allowed, there may be multiple schemas.
   * Must call {@link #createPlan()} first.
   * 
   * @return map from path to input schema
   */
  public Map<String,Schema> getInputSchemasByPath()
  {
    checkPlanExists();
    return _inputSchemasByPath;
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
    checkPlanExists();
    return _needAnotherPass;
  }
  
  /**
   * Gets the inputs which are to be processed.
   * Must call {@link #createPlan()} first.
   * 
   * @return inputs to process
   */
  public List<DatePath> getInputsToProcess()
  {
    checkPlanExists();
    return _inputsToProcess;
  }
  
  /**
   * Gets the input dates which are to be processed.
   * Must call {@link #createPlan()} first.
   * 
   * @return dates to process
   */
  public List<Date> getDatesToProcess()
  {
    checkPlanExists();
    Set<Date> dates = new TreeSet<Date>();
    for (DatePath dp : _inputsToProcess)
    {
      dates.add(dp.getDate());
    }
    return new ArrayList<Date>(dates);
  }
  
  /**
   * Determines the number of reducers to use based on the input data size.
   * The number of reducers to use is based on the input data size and the 
   * <em>num.reducers.bytes.per.reducer</em> property.  See {@link ReduceEstimator}
   * for details on reducer estimation.
   * 
   * @throws IOException
   */
  private void determineNumReducers() throws IOException
  {
    ReduceEstimator estimator = new ReduceEstimator(getFileSystem(),getProps());
    List<String> inputPaths = new ArrayList<String>();
    for (DatePath input : getInputsToProcess())
    {
      inputPaths.add(input.getPath().toString());
      estimator.addInputPath("input",input.getPath());
    }
    _numReducers = estimator.getNumReducers();
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
   * Determines which input data should be processed.  This checks the availability of input data within
   * the desired date range and also checks whether the output already exists.  Only inputs with no 
   * corresponding output are processed. 
   */
  private void determineInputsToProcess()
  {
    _log.info("Determining inputs to process");
    _latestInputByPath.clear();
    int newDataCount = 0;
    Calendar cal = Calendar.getInstance(PathUtils.timeZone);
    for (Date currentDate=getDateRange().getBeginDate(); currentDate.compareTo(getDateRange().getEndDate()) <= 0; )
    { 
      if (!_outputPathsByDate.containsKey(currentDate))
      {      
        List<DatePath> inputs = getAvailableInputsByDate().get(currentDate);  
        if (inputs != null)
        { 
          if (getMaxToProcess() != null && newDataCount >= getMaxToProcess())
          {          
            // too much data to process in a single run, will require another pass
            _needAnotherPass = true;
            break;
          }
          
          for (DatePath input : inputs)
          {
            _log.info(String.format("Input: %s",input.getPath()));
            _inputsToProcess.add(input);
            
            Path root = PathUtils.getNestedPathRoot(input.getPath());
            _latestInputByPath.put(root.toString(), input.getPath().toString());
          }
                    
          newDataCount++;
        }
        else
        {
          throw new RuntimeException("missing input data for " + currentDate);
        }
      }
      
      cal.setTime(currentDate);
      cal.add(Calendar.DAY_OF_MONTH, 1);
      currentDate = cal.getTime();
    }
  }
    
  /**
   * Determines what output data already exists.  Inputs will not be consumed if the output already exists.
   * 
   * @throws IOException
   */
  private void loadOutputData() throws IOException
  {
    _log.info(String.format("Checking output data in " + getOutputPath()));
    _outputPathsByDate = getDailyData(getOutputPath());
  }
  
  /**
   * Throws an exception if the plan hasn't been created.
   */
  private void checkPlanExists()
  {
    if (!_planExists) throw new RuntimeException("Must call createPlan first");
  }
}
