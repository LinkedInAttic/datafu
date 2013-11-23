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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


import datafu.hourglass.avro.CombinedAvroKeyInputFormat;
import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;

/**
 * Base class for Hadoop jobs that consume time-partitioned data
 * in a non-incremental way.  Typically this is only used for comparing incremental
 * jobs against a non-incremental baseline.
 * It is essentially the same as {@link AbstractPartitionCollapsingIncrementalJob}
 * without all the incremental features.
 * 
 * <p>
 * Jobs extending this class consume input data partitioned according to yyyy/MM/dd.
 * Only a single input path is supported.  The output will be written to a directory
 * in the output path with name format yyyyMMdd derived from the end of the time
 * window that is consumed.
 * </p>
 * 
 * <p>
 * This class has the same configuration and methods as {@link TimeBasedJob}.
 * In addition it also recognizes the following properties:
 * </p>
 * 
 *  <ul>
 *   <li><em>combine.inputs</em> - True if inputs should be combined (defaults to false)</li>
 *   <li><em>num.reducers.bytes.per.reducer</em> - Number of input bytes per reducer</li>
 * </ul>
 * 
 * <p>
 * When <em>combine.inputs</em> is true, then CombinedAvroKeyInputFormat is used
 * instead of AvroKeyInputFormat.  This enables a single map task to consume more than
 * one file.
 * </p>
 * 
 * <p>
 * The <em>num.reducers.bytes.per.reducer</em> property controls the number of reducers to 
 * use based on the input size.  The total size of the input files is divided by this number
 * and then rounded up.
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class AbstractNonIncrementalJob extends TimeBasedJob
{
  private final Logger _log = Logger.getLogger(AbstractNonIncrementalJob.class);
    
  private boolean _combineInputs;
  private Report _report;
  
  /**
   * Initializes the job.
   * 
   * @param name job name
   * @param props configuration properties
   * @throws IOException
   */
  public AbstractNonIncrementalJob(String name, Properties props) throws IOException
  {        
    super(name,props);
    
    if (props.containsKey("combine.inputs"))
    {
      setCombineInputs(Boolean.parseBoolean(props.getProperty("combine.inputs")));
    }
  }
  
  /**
   * Gets whether inputs should be combined.
   * 
   * @return true if inputs are to be combined
   */
  public boolean getCombineInputs()
  {
    return _combineInputs;
  }
  
  /**
   * Sets whether inputs should be combined.
   * 
   * @param combineInputs true to combine inputs
   */
  public void setCombineInputs(boolean combineInputs)
  {
    _combineInputs = combineInputs;
  }
  
  /**
   * Gets a report summarizing the run.
   * 
   * @return report
   */
  public Report getReport()
  {
    return _report;
  }
  
  /**
   * Runs the job.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Override
  public void run() throws IOException, InterruptedException, ClassNotFoundException
  {     
    _report = new Report();
    
    Calendar cal = Calendar.getInstance(PathUtils.timeZone);   
    
    if (!getFileSystem().exists(getOutputPath()))
    {
      getFileSystem().mkdirs(getOutputPath());
    }
    
    if (getInputPaths().size() > 1)
    {
      throw new RuntimeException("Only a single input is supported");
    }
    
    List<DatePath> inputs = PathUtils.findNestedDatedPaths(getFileSystem(), getInputPaths().get(0));
    
    DatePath latestInput = (inputs.size() > 0) ? inputs.get(inputs.size() - 1) : null; 
    
    if (inputs.size() == 0)
    {
      throw new RuntimeException("no input data available");
    }
    
    List<Date> dates = new ArrayList<Date>();
    for (DatePath dp : inputs)
    {
      dates.add(dp.getDate());
    }
    
    DateRange dateRange = DateRangePlanner.getDateRange(getStartDate(), getEndDate(), dates, getDaysAgo(), getNumDays());
    
    Map<Date,DatePath> existingInputs = new HashMap<Date,DatePath>();
    for (DatePath input : inputs)
    {
      existingInputs.put(input.getDate(), input);
    }
    
    _log.info("Getting schema for input " + latestInput.getPath());
    Schema inputSchema = PathUtils.getSchemaFromPath(getFileSystem(),latestInput.getPath());
    
    ReduceEstimator estimator = new ReduceEstimator(getFileSystem(),getProperties());
    
    List<String> inputPaths = new ArrayList<String>();
    for (Date currentDate=dateRange.getBeginDate(); currentDate.compareTo(dateRange.getEndDate()) <= 0; )
    { 
      DatePath input = existingInputs.get(currentDate);  
      if (input != null)
      { 
        _log.info(String.format("Processing %s",input.getPath()));
        inputPaths.add(input.getPath().toString());
        estimator.addInputPath(input.getPath());
        _report.inputFiles.add(input);
        latestInput = input;
      }
      else
      {
        throw new RuntimeException(String.format("Missing input for %s",PathUtils.datedPathFormat.format(currentDate)));
      }
      
      cal.setTime(currentDate);
      cal.add(Calendar.DAY_OF_MONTH, 1);
      currentDate = cal.getTime();
    }
        
    Path timestampOutputPath = new Path(getOutputPath(),PathUtils.datedPathFormat.format(latestInput.getDate()));
    
    final StagedOutputJob job = StagedOutputJob.createStagedJob(
                                          getConf(),
                                          getName() + "-" + PathUtils.datedPathFormat.format(latestInput.getDate()),            
                                          inputPaths,
                                          "/tmp" + timestampOutputPath.toString(),
                                          timestampOutputPath.toString(),
                                          _log);
    
    
    job.setCountersParentPath(getCountersParentPath());
    
    if (_combineInputs)
    {
      job.setInputFormatClass(CombinedAvroKeyInputFormat.class);
    }
    else
    {
      job.setInputFormatClass(AvroKeyInputFormat.class);
    }
    
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    
    AvroJob.setInputKeySchema(job, inputSchema);
    AvroJob.setMapOutputKeySchema(job, getMapOutputKeySchema());
    AvroJob.setMapOutputValueSchema(job, getMapOutputValueSchema());
    AvroJob.setOutputKeySchema(job, getReduceOutputSchema());
    
    int numReducers;
    if (getNumReducers() != null)
    {
      numReducers = getNumReducers();        
      _log.info(String.format("Using %d reducers (fixed)",numReducers));
    }
    else      
    {         
      numReducers = estimator.getNumReducers();        
      _log.info(String.format("Using %d reducers (computed)",numReducers));
    }
        
    job.setNumReduceTasks(numReducers);
    
    job.setMapperClass(getMapperClass());
    job.setReducerClass(getReducerClass());
    
    if (isUseCombiner() && getCombinerClass() != null)
    {
      job.setCombinerClass(getCombinerClass());
    }
    
    config(job.getConfiguration());
    
    if (!job.waitForCompletion(true))
    {
      _log.error("Job failed! Quitting...");
      throw new RuntimeException("Job failed");
    }
        
    _report.jobId = job.getJobID().toString();
    _report.jobName = job.getJobName();
    _report.countersPath = job.getCountersPath();
    _report.outputFile = new DatePath(latestInput.getDate(),timestampOutputPath);
    
    if (getRetentionCount() != null)
    {
      PathUtils.keepLatestDatedPaths(getFileSystem(), getOutputPath(), getRetentionCount());
    }   
  }
  
  /**
   * Gets the key schema for the map output.
   * 
   * @return map output key schema
   */
  protected abstract Schema getMapOutputKeySchema();
  
  /**
   * Gets the value schema for the map output.
   * 
   * @return map output value schema
   */
  protected abstract Schema getMapOutputValueSchema();
  
  /**
   * Gets the reduce output schema.
   * 
   * @return reduce output schema
   */
  protected abstract Schema getReduceOutputSchema();
  
  /**
   * Gets the mapper class.
   * 
   * @return the mapper
   */
  public abstract Class<? extends BaseMapper> getMapperClass();
 
  /**
   * Gets the reducer class.
   * 
   * @return the reducer
   */
  public abstract Class<? extends BaseReducer> getReducerClass();
  
  /**
   * Gets the combiner class.
   * 
   * @return the combiner
   */
  public Class<? extends BaseCombiner> getCombinerClass()
  {
    return null;
  }  
  
  /**
   * Mapper base class for {@link AbstractNonIncrementalJob}.
   * 
   * @author "Matthew Hayes"
   *
   */
  public static abstract class BaseMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, AvroValue<GenericRecord>>
  {    
  }
  
  /**
   * Combiner base class for {@link AbstractNonIncrementalJob}.
   * 
   * @author "Matthew Hayes"
   *
   */
  public static abstract class BaseCombiner extends Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, AvroValue<GenericRecord>>
  {    
  }
  
  /**
   * Reducer base class for {@link AbstractNonIncrementalJob}.
   * 
   * @author "Matthew Hayes"
   *
   */
  public static abstract class BaseReducer extends Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>
  {    
  }
  
  /**
   * Reports files created and processed for an iteration of the job.
   * 
   * @author "Matthew Hayes"
   *
   */
  public static class Report
  {
    private String jobName;
    private String jobId;
    private Path countersPath;
    private List<DatePath> inputFiles = new ArrayList<DatePath>();
    private DatePath outputFile;
    
    /**
     * Gets the job name.
     * 
     * @return job name
     */
    public String getJobName()
    {
      return jobName;
    }
    
    /**
     * Gets the job ID.
     * 
     * @return job ID
     */
    public String getJobId()    
    {
      return jobId;
    }
    
    /**
     * Gets the path to the counters file, if one was written.
     * 
     * @return counters path
     */
    public Path getCountersPath()
    {
      return countersPath;
    }
    
    /**
     * Gets input files that were processed.  These are files that are within
     * the desired date range.
     * 
     * @return input files
     */
    public List<DatePath> getInputFiles()
    {
      return Collections.unmodifiableList(inputFiles);
    }
    
    /**
     * Gets the output file that was produced by the job.
     * 
     * @return output file
     */
    public DatePath getOutputFile()
    {
      return outputFile;
    }
  }
}
