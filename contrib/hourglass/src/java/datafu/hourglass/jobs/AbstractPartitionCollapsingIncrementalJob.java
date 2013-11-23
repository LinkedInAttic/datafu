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
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.log4j.Logger;


import datafu.hourglass.avro.AvroDateRangeMetadata;
import datafu.hourglass.avro.AvroKeyWithMetadataOutputFormat;
import datafu.hourglass.avro.AvroMultipleInputsKeyInputFormat;
import datafu.hourglass.avro.AvroMultipleInputsUtil;
import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.mapreduce.AvroKeyValueIdentityMapper;
import datafu.hourglass.mapreduce.CollapsingCombiner;
import datafu.hourglass.mapreduce.CollapsingMapper;
import datafu.hourglass.mapreduce.CollapsingReducer;
import datafu.hourglass.mapreduce.DelegatingCombiner;
import datafu.hourglass.mapreduce.DelegatingMapper;
import datafu.hourglass.mapreduce.DelegatingReducer;
import datafu.hourglass.mapreduce.DistributedCacheHelper;
import datafu.hourglass.mapreduce.Parameters;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.model.Merger;
import datafu.hourglass.schemas.PartitionCollapsingSchemas;

/**
 * An {@link IncrementalJob} that consumes partitioned input data and collapses the
 * partitions to produce a single output.  This job can be used to process data
 * using a sliding window.  It is capable of reusing the previous output, which
 * means that it can process data more efficiently.
 * Only Avro is supported for the input, intermediate, and output data.
 * 
 * <p>
 * Implementations of this class must provide key, intermediate value, and output value schemas.
 * The key and intermediate value schemas define the output for the mapper and combiner.
 * The key and output value schemas define the output for the reducer.
 * These are defined by overriding {@link #getKeySchema()}, {@link #getIntermediateValueSchema()},
 * and {@link #getOutputValueSchema()}.
 * </p>
 * 
 * <p>
 * Implementations must also provide a mapper by overriding {@link #getMapper()} and an accumulator
 * for the reducer by overriding {@link #getReducerAccumulator()}.  An optional combiner may be
 * provided by overriding {@link #getCombinerAccumulator()}.  For the combiner to be used
 * the property <em>use.combiner</em> must also be set to true.
 * </p>
 * 
 * <p>
 * The input path can be provided either through the property <em>input.path</em>
 * or by calling {@link #setInputPaths(List)}.  If multiple input paths are provided then
 * this implicitly means a join is to be performed.  Multiple input paths can be provided via
 * properties by prefixing each with <em>input.path.</em>, such as <em>input.path.first</em>
 * and <em>input.path.second</em>.
 * Input data must be partitioned by day according to the naming convention yyyy/MM/dd.
 * The output path can be provided either through the property <em>output.path</em> 
 * or by calling {@link #setOutputPath(Path)}.
 * Output data will be written using the naming convention yyyyMMdd, where the date used
 * to format the output path is the same as the end of the desired time range to process.
 * For example, if the desired time range to process is 2013/01/01 through 2013/01/14,
 * then the output will be named 20130114. 
 * By default the job will fail if any input data in the desired time window is missing.  This can be overriden by setting
 * <em>fail.on.missing</em> to false.
 * </p>
 * 
 * <p>
 * The job will not process input if the corresponding output has already been produced.  For example, if the desired date
 * range is 2013/01/01 through 2013/01/14 and the output 20130114 already exists, then it assumes the work has alreaday
 * been completed.
 * </p>
 * 
 * <p>
 * By default only the latest output will be kept.  All other outputs will be removed.  This can be controlled
 * by setting the property <em>retention.count</em>, or by calling {@link #setRetentionCount(Integer)}.
 * </p>
 * 
 * <p>
 * Two types of sliding windows may be used: <em>fixed-length</em> and <em>fixed-start</em>.  For a fixed-length
 * sliding window, the size of the window is fixed; the start and end move according to the
 * availability of input data.  For a fixed-start window, the size of the window is flexible;
 * the start is fixed and the end moves according to the availability of input data.
 * </p>
 * 
 * <p>
 * A fixed-length sliding window can be defined either by setting the property <em>num.days</em>
 * or by calling {@link #setNumDays(Integer)}.  This sets how many days of input data will be
 * consumed.  By default the end of the window will be the same as the date of the latest available
 * input data.  The start is then determine by the number of days to consume.  The end date can
 * be moved back relative to the latest input data by setting the <em>days.ago</em> property or
 * by calling {@link #setDaysAgo(Integer)}.  Since the end date is determined by the availability
 * of input data, as new data arrives the window will advance forward.
 * </p>
 * 
 * <p>
 * A fixed-start sliding window can be defined by setting the property <em>start.date</em> or
 * by calling {@link #setStartDate(java.util.Date)}.  The end date will be the same as the date of
 * the latest available input data.  The end date can
 * be moved back relative to the latest input data by setting the <em>days.ago</em> property or
 * by calling {@link #setDaysAgo(Integer)}.
 * Because the end date is determined by the availability of input data, as new data arrives the window 
 * will grow to include it.
 * </p>
 * 
 * <p>
 * Previous output can be reused by setting the <em>reuse.previous.output</em> property to true, or
 * by calling {@link #setReusePreviousOutput(boolean)}.  Reusing the previous output is often more efficient
 * because only input data outside of the time window covered by the previous output needs to be consumed.
 * For example, given a fixed-start sliding window job, if one new day of input data is available since the
 * last time the job ran, then the job can reuse the previous output and only read the newest day of data, rather
 * than reading all the input data again.  Given a fixed-length sliding window in the same scenario, the new output
 * can be produced by adding the newest input to the previous output and subtracting the oldest input from the old
 * window.
 * </p>
 * 
 * <p>
 * For a fixed-start sliding window, if the schema for the intermediate and output values are the same then no additional
 * changes are necessary, as the reducer's accumulator should be capable of adding the new input to the previous output.
 * However if they are different then a record must be defined by overriding {@link #getRecordMerger()} so that the previous
 * output can be merged with the partial output produced by reducing the new input data.
 * For the fixed-length sliding window one must override {@link #getOldRecordMerger()} to reuse the previous output.
 * This method essentially unmerges old, partial output data from the current output.  For this case as well if the intermediate
 * and output schemas are the same the {@link #getRecordMerger()} method does not need to be overriden.
 * </p>
 * 
 * <p>
 * The number of reducers to use is automatically determined based on the size of the data to process.  
 * The total size is computed and then divided by the value of the property <em>num.reducers.bytes.per.reducer</em>, which
 * defaults to 256 MB.  This is the number of reducers that will be used.  This calculation includes
 * the input data as well as previous output that will be reused.    It is also possible calculate the number of reducers
 * separately for the input and previous output through the properties <em>num.reducers.input.bytes.per.reducer</em>
 * and <em>num.reducers.previous.bytes.per.reducer</em>.  The reducers will be computed separately for the two sets of data
 * and then added together.  The number of reducers can also be set to a fixed value through the property <em>num.reducers</em>.
 * </p>
 * 
 * <p>
 * This type of job is capable of performing its work over multiple iterations if previous output can be reused.  
 * The number of days to process at a time can be limited by setting the property <em>max.days.to.process</em>,
 * or by calling {@link #setMaxToProcess(Integer)}.  The default is 90 days.  
 * This can be useful when there are restrictions on how many tasks 
 * can be used by a single MapReduce job in the cluster.  When this property is set, the job will process no more than
 * this many days at a time, and it will perform one or more iterations if necessary to complete the work.
 * The number of iterations can be limited by setting the property <em>max.iterations</em>, or by calling {@link #setMaxIterations(Integer)}.
 * If the number of iterations is exceeded the job will fail.  By default the maximum number of iterations is 20.
 * </p>
 *  
 * <p>
 * Hadoop configuration may be provided by setting a property with the prefix <em>hadoop-conf.</em>.
 * For example, <em>mapred.min.split.size</em> can be configured by setting property
 * <em>hadoop-conf.mapred.min.split.size</em> to the desired value. 
 * </p>
 * 
 * @author "Matthew Hayes"
 *
 */
public abstract class AbstractPartitionCollapsingIncrementalJob extends IncrementalJob
{
  private final Logger _log = Logger.getLogger(AbstractPartitionCollapsingIncrementalJob.class);
  
  private List<Report> _reports = new ArrayList<Report>();
  protected boolean _reusePreviousOutput;
  private FileCleaner _garbage;
  
  /**
   * Initializes the job.
   */
  public AbstractPartitionCollapsingIncrementalJob() throws IOException
  {    
  }
  
  /**
   * Initializes the job with a job name and properties.
   * 
   * @param name job name
   * @param props configuration properties
   */
  public AbstractPartitionCollapsingIncrementalJob(String name, Properties props) throws IOException
  { 
    super(name,props);
  }
  
  /**
   * Gets the mapper.
   * 
   * @return mapper
   */
  public abstract Mapper<GenericRecord,GenericRecord,GenericRecord> getMapper();
  
  /**
   * Gets the accumulator used for the combiner.
   * 
   * @return combiner accumulator
   */
  public Accumulator<GenericRecord,GenericRecord> getCombinerAccumulator()
  {
    return null;
  }
  
  /**
   * Gets the accumulator used for the reducer.
   * 
   * @return reducer accumulator
   */
  public abstract Accumulator<GenericRecord,GenericRecord> getReducerAccumulator(); 
  
  /**
   * Gets the record merger that is capable of merging previous output with a new partial output.
   * This is only needed when reusing previous output where the intermediate and output schemas are different.
   * New partial output is produced by the reducer from new input that is after the previous output.
   * 
   * @return merger
   */
  public Merger<GenericRecord> getRecordMerger()
  {
    return null;
  }

  /**
   * Gets the record merger that is capable of unmerging old partial output from the new output.
   * This is only needed when reusing previous output for a fixed-length sliding window.
   * The new output is the result of merging the previous output with the new partial output.
   * The old partial output is produced by the reducer from old input data before the time range of
   * the previous output. 
   * 
   * @return merger
   */
  public Merger<GenericRecord> getOldRecordMerger()
  {
    return null;
  }
  
  /**
   * Get the name for the reduce output schema. 
   * By default this is the name of the class with "Output" appended.
   * 
   * @return output schema name
   */
  protected String getOutputSchemaName()
  {
    return this.getClass().getSimpleName() + "Output";
  }
  
  /**
   * Get the namespace for the reduce output schema.
   * By default this is the package of the class.
   * 
   * @return output schema namespace
   */
  protected String getOutputSchemaNamespace()
  {
    return this.getClass().getPackage().getName();
  }
  
  @Override
  public void setProperties(Properties props)
  {
    super.setProperties(props);
    
    if (getProperties().get("reuse.previous.output") != null)
    {
      setReusePreviousOutput(Boolean.parseBoolean((String)getProperties().get("reuse.previous.output")));
    }
  }
  
  /**
   * Get whether previous output should be reused.
   * 
   * @return true if previous output should be reused
   */
  public boolean getReusePreviousOutput()
  {
    return _reusePreviousOutput;
  }
  
  /**
   * Set whether previous output should be reused.
   * 
   * @param reuse true if previous output should be reused
   */
  public void setReusePreviousOutput(boolean reuse)
  {
    _reusePreviousOutput = reuse;
  }
  
  @Override
  protected void initialize()
  { 
    _garbage = new FileCleaner(getFileSystem());
    
    if (getMaxIterations() == null)
    {
      setMaxIterations(20);
    }
    
    if (getMaxToProcess() == null)
    {
      if (getNumDays() != null)
      {
        setMaxToProcess(getNumDays());
      }
      else
      {
        setMaxToProcess(90);
      }
    }
    
    if (getRetentionCount() == null)
    {
      setRetentionCount(1);
    }
    
    super.initialize();
  }
  
  @Override
  public void run() throws IOException, InterruptedException, ClassNotFoundException
  {
    try
    {
      initialize();
      validate();
      execute();
    }
    finally
    {
      cleanup();
    }
  }  
  
  /**
   * Get reports that summarize each of the job iterations.
   * 
   * @return reports
   */
  public List<Report> getReports()
  {
    return Collections.unmodifiableList(_reports);
  }
  
  /**
   * Execute the job.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private void execute() throws IOException, InterruptedException, ClassNotFoundException
  { 
    int iterations = 0;
    
    while (true)
    {
      PartitionCollapsingExecutionPlanner planner = new PartitionCollapsingExecutionPlanner(getFileSystem(),getProperties());
      planner.setInputPaths(getInputPaths());
      planner.setOutputPath(getOutputPath());
      planner.setStartDate(getStartDate());
      planner.setEndDate(getEndDate());
      planner.setDaysAgo(getDaysAgo());
      planner.setNumDays(getNumDays());
      planner.setMaxToProcess(getMaxToProcess());
      planner.setReusePreviousOutput(getReusePreviousOutput());
      planner.setFailOnMissing(isFailOnMissing());
      planner.createPlan();      
      
      if (planner.getInputsToProcess().size() == 0)
      {
        _log.info("Nothing to do");
        break;
      }
      
      if (iterations >= getMaxIterations())
      {
        throw new RuntimeException(String.format("Already completed %d iterations but the max is %d and there are still %d inputs to process",
                                                 iterations,
                                                 getMaxIterations(),
                                                 planner.getInputsToProcess().size()));
      }
      
      Report report = new Report();
      
      report.inputFiles.addAll(planner.getNewInputsToProcess());
      report.oldInputFiles.addAll(planner.getOldInputsToProcess());
      if (planner.getPreviousOutputToProcess() != null)
      {
        report.reusedOutput = planner.getPreviousOutputToProcess();
      }
      
      DatePath outputPath = DatePath.createDatedPath(getOutputPath(), planner.getCurrentDateRange().getEndDate());
      
      _log.info("Output path: " + outputPath);
      
      Path tempOutputPath = createRandomTempPath();  
      
      _garbage.add(tempOutputPath);
            
      final StagedOutputJob job = StagedOutputJob.createStagedJob(
          getConf(),
          getName() + "-" + PathUtils.datedPathFormat.format(planner.getCurrentDateRange().getEndDate()),            
          null, // no input paths specified here, will add multiple inputs down below
          tempOutputPath.toString(),
          outputPath.getPath().toString(),
          _log);
      
      job.setCountersParentPath(getCountersParentPath());

      if (planner.getNewInputsToProcess() != null && planner.getNewInputsToProcess().size() > 0)
      {
        _log.info("*** New Input data:");
        for (DatePath inputPath : planner.getNewInputsToProcess())
        {
          _log.info(inputPath.getPath());
          MultipleInputs.addInputPath(job, inputPath.getPath(), AvroMultipleInputsKeyInputFormat.class, DelegatingMapper.class);
        }
      }
      
      if (planner.getOldInputsToProcess() != null && planner.getOldInputsToProcess().size() > 0)
      {
        _log.info("*** Old Input data:");
        for (DatePath inputPath : planner.getOldInputsToProcess())
        {
          _log.info(inputPath.getPath());
          MultipleInputs.addInputPath(job, inputPath.getPath(), AvroMultipleInputsKeyInputFormat.class, DelegatingMapper.class);
        }
      }
      
      if (planner.getPreviousOutputToProcess() != null)
      {
        _log.info("*** Previous output data:");
        _log.info(planner.getPreviousOutputToProcess().getPath());
        MultipleInputs.addInputPath(job, planner.getPreviousOutputToProcess().getPath(), AvroKeyInputFormat.class, AvroKeyValueIdentityMapper.class);
      }
      
      final Configuration conf = job.getConfiguration();
      
      config(conf);
      
      AvroDateRangeMetadata.configureOutputDateRange(conf, planner.getCurrentDateRange());
                  
      PartitionCollapsingSchemas spSchemas = new PartitionCollapsingSchemas(getSchemas(), planner.getInputSchemasByPath(), getOutputSchemaName(), getOutputSchemaNamespace());
      
      job.setOutputFormatClass(AvroKeyWithMetadataOutputFormat.class);
      
      _log.info("Setting input path to schema mappings");
      for (String path : spSchemas.getMapInputSchemas().keySet())
      {
        Schema schema = spSchemas.getMapInputSchemas().get(path);
        _log.info("*** " + path);
        _log.info("*** => " + schema.toString());
        AvroMultipleInputsUtil.setInputKeySchemaForPath(job, schema, path);
      }
      
      AvroJob.setMapOutputKeySchema(job, spSchemas.getMapOutputKeySchema());
      AvroJob.setMapOutputValueSchema(job, spSchemas.getMapOutputValueSchema());
      AvroJob.setOutputKeySchema(job, spSchemas.getReduceOutputSchema());
                    
      int numReducers;
            
      if (getNumReducers() != null)
      {
        numReducers = getNumReducers();        
        _log.info(String.format("Using %d reducers (fixed)",numReducers));
      }
      else      
      {         
        numReducers = planner.getNumReducers();      
        _log.info(String.format("Using %d reducers (computed)",numReducers));
      }
          
      job.setNumReduceTasks(numReducers);
      
      job.setReducerClass(DelegatingReducer.class);
      
      Path mapperPath = new Path(tempOutputPath,".mapper_impl");
      Path reducerPath = new Path(tempOutputPath,".reducer_impl");
      Path combinerPath = new Path(tempOutputPath,".combiner_impl");
      
      CollapsingMapper mapper = new CollapsingMapper();
      CollapsingReducer reducer = new CollapsingReducer();
      
      mapper.setSchemas(spSchemas);
      reducer.setSchemas(spSchemas);
      
      mapper.setMapper(getMapper());
      reducer.setAccumulator(getReducerAccumulator());
      reducer.setRecordMerger(getRecordMerger());
      reducer.setOldRecordMerger(getOldRecordMerger());

      mapper.setReuseOutput(_reusePreviousOutput);
      reducer.setReuseOutput(_reusePreviousOutput);
      
      configureOutputDateRange(job.getConfiguration(),planner.getCurrentDateRange(), reducer);
      
      DistributedCacheHelper.writeObject(conf, mapper, mapperPath);
      DistributedCacheHelper.writeObject(conf, reducer, reducerPath);
      
      conf.set(Parameters.REDUCER_IMPL_PATH, reducerPath.toString());
      conf.set(Parameters.MAPPER_IMPL_PATH, mapperPath.toString());
      
      if (isUseCombiner())
      {
        CollapsingCombiner combiner = new CollapsingCombiner();
        configureOutputDateRange(job.getConfiguration(),planner.getCurrentDateRange(), combiner);
        combiner.setReuseOutput(_reusePreviousOutput);
        combiner.setSchemas(spSchemas);
        combiner.setAccumulator(getCombinerAccumulator());
        conf.set(Parameters.COMBINER_IMPL_PATH, combinerPath.toString());
        job.setCombinerClass(DelegatingCombiner.class);
        DistributedCacheHelper.writeObject(conf, combiner, combinerPath);
      }
      
      if (!job.waitForCompletion(true))
      {
        _log.error("Job failed! Quitting...");
        throw new RuntimeException("Job failed");
      }
      
      report.jobId = job.getJobID().toString();
      report.jobName = job.getJobName();
      report.countersPath = job.getCountersPath();
      report.outputPath = outputPath;
      
      _reports.add(report);
      
      applyRetention();
      
      if (!planner.getNeedsAnotherPass())
      {
        break;
      }
      
      cleanup();
      
      iterations++;
    }
  }
  
  /**
   * Removes all but the more recent ouputs that are within the retention period, if one is specified.
   * 
   * @throws IOException
   */
  private void applyRetention() throws IOException
  {
    if (getRetentionCount() != null)
    {
      PathUtils.keepLatestDatedPaths(getFileSystem(), getOutputPath(), getRetentionCount());
    }
  }
  
  /**
   * Configures the output date range for processing components. 
   * 
   * @param conf configuration
   * @param dateRange output date range
   * @param proc processor
   */
  private static void configureOutputDateRange(Configuration conf, DateRange dateRange, DateRangeConfigurable proc)
  {
    Calendar cal = Calendar.getInstance(PathUtils.timeZone);
    long beginTime = 0L;
    long endTime = Long.MAX_VALUE;
    
    if (dateRange.getBeginDate() != null)
    { 
      cal.setTime(dateRange.getBeginDate());
      beginTime = cal.getTimeInMillis();
    }
    
    if (dateRange.getEndDate() != null)
    {
      cal.setTime(dateRange.getEndDate());
      cal.getTimeInMillis();
    }
    
    proc.setOutputDateRange(new DateRange(new Date(beginTime),new Date(endTime)));
  }
  
  /**
   * Remove all temporary paths. 
   * 
   * @throws IOException
   */
  private void cleanup() throws IOException
  {
    if (_garbage != null)
    {
      _garbage.clean();
    }
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
    private DatePath outputPath;
    private List<DatePath> inputFiles = new ArrayList<DatePath>();
    private List<DatePath> oldInputFiles = new ArrayList<DatePath>();
    private DatePath reusedOutput;
    
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
     * Gets the path to the output which was produced by the job. 
     * 
     * @return output path
     */
    public DatePath getOutputPath()
    {
      return outputPath;
    }
    
    /**
     * Gets the output that was reused, if one was reused. 
     * 
     * @return reused output path
     */
    public DatePath getReusedOutput()
    {
      return reusedOutput;
    }
    
    /**
     * Gets new input files that were processed.  These are files that are within
     * the desired date range.
     * 
     * @return input files
     */
    public List<DatePath> getInputFiles()
    {
      return Collections.unmodifiableList(inputFiles);
    }
    
    /**
     * Gets old input files that were processed.  These are files that are before
     * the desired date range and were subtracted from the reused output.
     * 
     * @return output files
     */
    public List<DatePath> getOldInputFiles()
    {
      return Collections.unmodifiableList(oldInputFiles);
    }
  }
}
