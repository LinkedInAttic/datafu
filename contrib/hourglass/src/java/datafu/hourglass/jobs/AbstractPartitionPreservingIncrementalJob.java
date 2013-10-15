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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;


import datafu.hourglass.avro.AvroMultipleInputsKeyInputFormat;
import datafu.hourglass.avro.AvroMultipleInputsUtil;
import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.mapreduce.DelegatingCombiner;
import datafu.hourglass.mapreduce.DelegatingMapper;
import datafu.hourglass.mapreduce.DelegatingReducer;
import datafu.hourglass.mapreduce.DistributedCacheHelper;
import datafu.hourglass.mapreduce.ObjectMapper;
import datafu.hourglass.mapreduce.ObjectReducer;
import datafu.hourglass.mapreduce.Parameters;
import datafu.hourglass.mapreduce.PartitioningCombiner;
import datafu.hourglass.mapreduce.PartitioningMapper;
import datafu.hourglass.mapreduce.PartitioningReducer;
import datafu.hourglass.model.Accumulator;
import datafu.hourglass.model.Mapper;
import datafu.hourglass.schemas.PartitionPreservingSchemas;

/**
 * An {@link IncrementalJob} that consumes partitioned input data and produces
 * output data having the same partitions.
 * Typically this is used in conjunction with {@link AbstractPartitionCollapsingIncrementalJob}
 * when computing aggregates over sliding windows.  A partition-preserving job can perform
 * initial aggregation per-day, which can then be consumed by a partition-collapsing job to
 * produce the final aggregates over the time window. 
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
 * The distinguishing feature this type of job is that the input partitioning is preserved in the ouput.
 * The data from each partition is processed independently of other partitions and then output separately.
 * For example, input that is partitioned by day can be aggregated by day and then output by day.
 * This is achieved by attaching a long value to each key, which represents the partition, so that the reducer
 * receives data grouped by the key and partition together.  Multiple outputs are then used so that the output
 * will have the same partitions as the input.
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
 * Output data will be written using the same naming convention as the input, namely yyyy/MM/dd, where the date used
 * to format the output path is the same the date for the input it was derived from.
 * For example, if the desired time range to process is 2013/01/01 through 2013/01/14,
 * then the output will be named 2013/01/01 through 2013/01/14. 
 * By default the job will fail if any input data in the desired time window is missing.  This can be overriden by setting
 * <em>fail.on.missing</em> to false.
 * </p>
 * 
 * <p>
 * The job will not process input for which a corresponding output already exists.  For example, if the desired date
 * range is 2013/01/01 through 2013/01/14 and the outputs 2013/01/01 through 2013/01/12 exist, then only
 * 2013/01/13 and 2013/01/14 will be processed and only 2013/01/13 and 2013/01/14 will be produced.  
 * </p>
 * 
 * <p>
 * The number of paths in the output to retain can be configured through the property <em>retention.count</em>, 
 * or by calling {@link #setRetentionCount(Integer)}.  When this property is set only the latest paths in the output
 * will be kept; the remainder will be removed.  By default there is no retention count set so all output paths are kept.
 * </p>
 * 
 * <p>
 * The inputs to process can be controlled by defining a desired date range.  By default the job will process all input
 * data available.  To limit the number of days of input to process one can set the property <em>num.days</em>
 * or call {@link #setNumDays(Integer)}.  This would define a processing window with the same number of days,
 * where the end date of the window is the latest available input and the start date is <em>num.days</em> ago.  
 * Only inputs within this window would be processed.
 * Because the end date is the same as the latest available input, as new input data becomes available the end of the
 * window will advance forward to include it.  The end date can be adjusted backwards relative to the latest input
 * through the property <em>days.ago</em>, or by calling {@link #setDaysAgo(Integer)}.  This subtracts as many days
 * from the latest available input date to determine the end date.  The start date or end date can also be fixed
 * by setting the properties <em>start.date</em> or <em>end.date</em>, or by calling {@link #setStartDate(Date)}
 * or {@link #setEndDate(Date)}. 
 * </p>
 * 
 * <p>
 * The number of reducers to use is automatically determined based on the size of the data to process.  
 * The total size is computed and then divided by the value of the property <em>num.reducers.bytes.per.reducer</em>, which
 * defaults to 256 MB.  This is the number of reducers that will be used.  
 * The number of reducers can also be set to a fixed value through the property <em>num.reducers</em>.
 * </p>
 * 
 * <p>
 * This type of job is capable of performing its work over multiple iterations.  
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
public abstract class AbstractPartitionPreservingIncrementalJob extends IncrementalJob
{
  private final Logger _log = Logger.getLogger(AbstractPartitionPreservingIncrementalJob.class);
  
  private List<Report> _reports = new ArrayList<Report>();
  private PartitioningMapper _mapper;
  private PartitioningCombiner _combiner;
  private PartitioningReducer _reducer;
  private FileCleaner _garbage;
  
  /**
   * Initializes the job.
   */
  public AbstractPartitionPreservingIncrementalJob() throws IOException
  {     
  }
  
  /**
   * Initializes the job with a job name and properties.
   * 
   * @param name job name
   * @param props configuration properties
   */
  public AbstractPartitionPreservingIncrementalJob(String name, Properties props) throws IOException
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
   * Run the job.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
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
    
    super.initialize();
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
  
  protected ObjectMapper getMapProcessor()
  {
    return _mapper;
  }
  
  protected ObjectReducer getCombineProcessor()
  {
    return _combiner;
  }
  
  protected ObjectReducer getReduceProcessor()
  {
    return _reducer;
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
    
    while(true)
    {
      PartitionPreservingExecutionPlanner planner = new PartitionPreservingExecutionPlanner(getFileSystem(),getProperties());
      planner.setInputPaths(getInputPaths());
      planner.setOutputPath(getOutputPath());
      planner.setStartDate(getStartDate());
      planner.setEndDate(getEndDate());
      planner.setDaysAgo(getDaysAgo());
      planner.setNumDays(getNumDays());
      planner.setMaxToProcess(getMaxToProcess());
      planner.setFailOnMissing(isFailOnMissing());
      planner.createPlan();
      
      if (planner.getInputsToProcess().size() == 0)
      {
        _log.info("Found all necessary incremental data");
        break;
      }
      
      if (iterations >= getMaxIterations())
      {
        throw new RuntimeException(String.format("Already completed %d iterations but the max is %d and there are still %d inputs to process",
                                                 iterations,
                                                 getMaxIterations(),
                                                 planner.getInputsToProcess().size()));
      }
      
      Path jobTempPath = createRandomTempPath();  
      _garbage.add(jobTempPath);
      ensurePath(getOutputPath());
      
      Path incrementalStagingPath = ensurePath(new Path(jobTempPath,".incremental-staging"));
      Path incrementalStagingTmpPath = ensurePath(new Path(jobTempPath,".incremental-staging-tmp"));
      
      Report report = new Report();    
           
      // create input paths for job
      List<String> inputPaths = new ArrayList<String>();
      for (DatePath input : planner.getInputsToProcess())
      {
        inputPaths.add(input.getPath().toString());
        report.inputFiles.add(input);
      }
      
      _log.info("Staging path: " + incrementalStagingPath);
      final StagedOutputJob job = StagedOutputJob.createStagedJob(
                                    getConf(),
                                    getName() + "-" + "incremental",            
                                    inputPaths,
                                    incrementalStagingTmpPath.toString(),
                                    incrementalStagingPath.toString(),
                                    _log);
              
      job.setCountersParentPath(getCountersParentPath());
      
      final Configuration conf = job.getConfiguration();
      
      config(conf);
      
      PartitionPreservingSchemas fpSchemas = new PartitionPreservingSchemas(getSchemas(), planner.getInputSchemasByPath(), getOutputSchemaName(), getOutputSchemaNamespace() );
      
      job.setInputFormatClass(AvroMultipleInputsKeyInputFormat.class);
      
      job.setOutputFormatClass(AvroKeyOutputFormat.class);
      
      _log.info("Setting input path to schema mappings");
      for (String path : fpSchemas.getMapInputSchemas().keySet())
      {
        Schema schema = fpSchemas.getMapInputSchemas().get(path);
        _log.info("*** " + path);
        _log.info("*** => " + schema.toString());
        AvroMultipleInputsUtil.setInputKeySchemaForPath(job, schema, path);
      }
            
      AvroJob.setMapOutputKeySchema(job, fpSchemas.getMapOutputKeySchema());
      AvroJob.setMapOutputValueSchema(job, fpSchemas.getMapOutputValueSchema());
      AvroJob.setOutputKeySchema(job, fpSchemas.getReduceOutputSchema());
            
      StringBuilder inputTimesJoined = new StringBuilder();
      for (Date input : planner.getDatesToProcess())
      {
        String namedOutput = PathUtils.datedPathFormat.format(input);
        _log.info(String.format("Adding named output %s",namedOutput));
        AvroMultipleOutputs.addNamedOutput(job, 
                                           namedOutput, 
                                           AvroKeyOutputFormat.class, 
                                           fpSchemas.getReduceOutputSchema());
        
        inputTimesJoined.append(Long.toString(input.getTime()));
        inputTimesJoined.append(",");
      }
      
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
      
      int avgReducersPerInput = (int)Math.ceil(numReducers/(double)planner.getDatesToProcess().size());
      
      _log.info(String.format("Reducers per input path: %d", avgReducersPerInput));    
      
      // counters for multiple outputs
//      conf.set("mo.counters", "true");
      
      conf.set(TimePartitioner.REDUCERS_PER_INPUT, Integer.toString(avgReducersPerInput));
      conf.set(TimePartitioner.INPUT_TIMES, inputTimesJoined.substring(0,inputTimesJoined.length()-1));    
              
      job.setNumReduceTasks(numReducers);
      
      Path mapperPath = new Path(incrementalStagingPath,".mapper_impl");
      Path reducerPath = new Path(incrementalStagingPath,".reducer_impl");
      Path combinerPath = new Path(incrementalStagingPath,".combiner_impl");
      
      conf.set(Parameters.REDUCER_IMPL_PATH, reducerPath.toString());
      conf.set(Parameters.MAPPER_IMPL_PATH, mapperPath.toString());
      
      _mapper = new PartitioningMapper();
      _mapper.setSchemas(fpSchemas);
      _mapper.setMapper(getMapper());

      _reducer = new PartitioningReducer();
      _reducer.setSchemas(fpSchemas);
      _reducer.setAccumulator(getReducerAccumulator());
      
      DistributedCacheHelper.writeObject(conf, getMapProcessor(), mapperPath);
      DistributedCacheHelper.writeObject(conf, getReduceProcessor(), reducerPath);
      
      job.setMapperClass(DelegatingMapper.class);
      job.setReducerClass(DelegatingReducer.class);
      
      if (isUseCombiner())
      {
        _combiner = new PartitioningCombiner();
        _combiner.setAccumulator(getCombinerAccumulator());
        conf.set(Parameters.COMBINER_IMPL_PATH, combinerPath.toString());
        job.setCombinerClass(DelegatingCombiner.class);
        DistributedCacheHelper.writeObject(conf, getCombineProcessor(), combinerPath);
      }
      
      job.setPartitionerClass(TimePartitioner.class);
            
      if (!job.waitForCompletion(true))
      {
        _log.error("Job failed! Quitting...");
        throw new RuntimeException("Job failed");
      }
      
      report.jobName = job.getJobName();
      report.jobId = job.getJobID().toString();
        
      moveStagedFiles(report,incrementalStagingPath);
      
      if (getCountersParentPath() == null)
      {
        // save the counters in the target path, for lack of a better place to put it
        Path counters = job.getCountersPath();
        if (getFileSystem().exists(counters))
        {
          Path target = new Path(getOutputPath(),counters.getName());
          if (getFileSystem().exists(target))
          {
            _log.info(String.format("Removing old counters at %s",target));
            getFileSystem().delete(target, true);
          }
          _log.info(String.format("Moving %s to %s",counters.getName(),getOutputPath()));
          getFileSystem().rename(counters, target);
          
          report.countersPath = target;
        }
        else
        {
          _log.error("Could not find counters at " + counters);
        }
      }

      applyRetention();
              
      _reports.add(report);
      
      if (!planner.getNeedsAnotherPass())
      {
        break;
      }
      
      cleanup();
      
      iterations++;
    }
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
   * Removes all but the more recent days from the ouput that are within the retention period, if one is specified.
   * 
   * @throws IOException
   */
  private void applyRetention() throws IOException
  {
    if (getRetentionCount() != null)
    {
      PathUtils.keepLatestNestedDatedPaths(getFileSystem(), getOutputPath(), getRetentionCount());
    }
  }
  
  /**
   * Moves files from the staging path to the final output path.
   * 
   * @param report report to update with output paths
   * @param sourcePath source of data to move
   * @throws IOException
   */
  private void moveStagedFiles(Report report, Path sourcePath) throws IOException
  {
    _log.info("Following files produced in staging path:");
    for (FileStatus stat : getFileSystem().globStatus(new Path(sourcePath,"*.avro")))
    {
      _log.info(String.format("* %s (%d bytes)",stat.getPath(),stat.getLen()));
    }
    
    FileStatus[] incrementalParts = getFileSystem().globStatus(new Path(sourcePath,"*"), new PathFilter() {
      @Override
      public boolean accept(Path path)
      {
        String[] pathParts = path.getName().split("-");
        try
        {
          Long.parseLong(pathParts[0]);
          return true;
        }
        catch (NumberFormatException e)
        {
          return false;
        }
      }
    });
    
    // collect the new incremental data from the temp folder and move to subfolders
    Map<String,Path> incrementalTargetPaths = new HashMap<String,Path>();
    for (FileStatus stat : incrementalParts)
    {        
      String[] pathParts = stat.getPath().getName().split("-");
      try
      {        
        String timestamp = pathParts[0];
        
        if (!incrementalTargetPaths.containsKey(timestamp))
        {
          Path parent = new Path(sourcePath,timestamp);
          
          if (!getFileSystem().exists(parent))
          {
            getFileSystem().mkdirs(parent);
          }
          else
          {
            throw new RuntimeException("already exists: " + parent.toString());
          }
          
          incrementalTargetPaths.put(timestamp,parent);
        }
        
        Path parent = incrementalTargetPaths.get(timestamp);
        _log.info(String.format("Moving %s to %s",stat.getPath().getName(),parent.toString()));
        getFileSystem().rename(stat.getPath(), new Path(parent,stat.getPath().getName()));
      }
      catch (NumberFormatException e)
      {
        throw new RuntimeException(e);
      }
    }
    
    for (Path src : incrementalTargetPaths.values())
    {
      Date srcDate;
      try
      {
        srcDate = PathUtils.datedPathFormat.parse(src.getName());
      }
      catch (ParseException e)
      {
        throw new RuntimeException(e);
      }
      Path target = new Path(getOutputPath(),PathUtils.nestedDatedPathFormat.format(srcDate));
      _log.info(String.format("Moving %s to %s",src.getName(),target));
      
      getFileSystem().mkdirs(target.getParent());
      
      if (!getFileSystem().rename(src, target))
      {
        throw new RuntimeException("Failed to rename " + src + " to " + target);
      }
      
      report.outputFiles.add(new DatePath(srcDate,target));
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
    private List<DatePath> inputFiles = new ArrayList<DatePath>();
    private List<DatePath> outputFiles = new ArrayList<DatePath>();
    
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
     * @return new input files
     */
    public List<DatePath> getInputFiles()
    {
      return Collections.unmodifiableList(inputFiles);
    }
    
    /**
     * Gets the output files that were produced by the job.
     * 
     * @return old input files
     */
    public List<DatePath> getOutputFiles()
    {
      return Collections.unmodifiableList(outputFiles);
    }
  }
}
