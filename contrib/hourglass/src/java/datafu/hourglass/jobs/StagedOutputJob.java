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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A derivation of {@link Job} that stages its output in another location and only
 * moves it to the final destination if the job completes successfully.
 * It also outputs a counters file to the file system that contains counters fetched from Hadoop
 * and other task statistics.
 */
public class StagedOutputJob extends Job implements Callable<Boolean>
{
  private final String _stagingPrefix;
  private final Logger _log;
  private Path _countersPath;
  private Path _countersParentPath;
  private boolean _writeCounters = true;
  
  /**
   * Creates a job which using a temporary staging location for the output data.
   * The data is only copied to the final output directory on successful completion
   * of the job.  This prevents existing output data from being overwritten unless
   * the job completes successfully.
   * 
   * @param conf configuration
   * @param jobName job name
   * @param inputPaths input paths
   * @param stagingLocation where to stage output temporarily
   * @param outputPath output path
   * @param log logger 
   * @return job
   */
  public static StagedOutputJob createStagedJob(
    Configuration conf,
    String jobName,
    List<String> inputPaths,
    String stagingLocation,
    String outputPath,
    final Logger log)
  {
    final StagedOutputJob retVal;
    try 
    {
      retVal = new StagedOutputJob(conf, stagingLocation, log);
      retVal.setJobName(jobName);
      retVal.setJarByClass(getCallersClass());
      FileInputFormat.setInputPathFilter(retVal, HiddenFilePathFilter.class);
    }
    catch (IOException e) 
    {
      log.error("IOException when making a job", e);
      throw new RuntimeException(e);
    }

    if (inputPaths != null)
    {
      try 
      {
        FileInputFormat.setInputPaths(
          retVal,
          StringUtils.join(inputPaths.iterator(),",")
        );
      }
      catch (IOException e) 
      {
          log.error("Unable to set up input paths.", e);
          throw new RuntimeException(e);
      }
    }
            
    FileOutputFormat.setOutputPath(retVal, new Path(outputPath));

    return retVal;
  }

  /**
   * Initializes the job.
   * 
   * @param conf configuration
   * @param stagingPrefix where to stage output temporarily
   * @param log logger
   * @throws IOException
   */
  public StagedOutputJob(Configuration conf, String stagingPrefix, Logger log) throws IOException
  {
    super(conf);
    this._stagingPrefix = stagingPrefix;
    this._log = log;
  }
  
  /**
   * Gets path to store the counters.  If this is not set then by default the counters will be
   * stored in the output directory.
   * 
   * @return path parent path for counters
   */
  public Path getCountersParentPath()
  {
    return _countersParentPath;
  }
   
  /**
   * Sets path to store the counters.  If this is not set then by default the counters will be
   * stored in the output directory.
   * 
   * @param path parent path for counters
   */
  public void setCountersParentPath(Path path)
  {
    _countersParentPath = path;
  }
  
  /**
   * Path to written counters.
   * 
   * @return counters path
   */
  public Path getCountersPath()
  {
    return _countersPath;
  }
  
  /**
   * Get whether counters should be written.
   * 
   * @return true if counters should be written
   */
  public boolean getWriteCounters()
  {
    return _writeCounters;
  }
  
  /**
   * Sets whether counters should be written.
   * 
   * @param writeCounters true if counters should be written
   */
  public void setWriteCounters(boolean writeCounters)
  {
    this._writeCounters = writeCounters;
  }

  /**
   * Run the job.
   */
  @Override
  public Boolean call() throws Exception
  {
    try
    {
      boolean success = false;    
      success = waitForCompletion(false);      
      String jobId = "?";
      
      if (getJobID() != null)
      {
        jobId = String.format("job_%s_%d",getJobID().getJtIdentifier(), getJobID().getId());
      }
      
      if (success)
      {
        _log.info(String.format("Job %s with ID %s succeeded! Tracking URL: %s", getJobName(), jobId, this.getTrackingURL()));
      }
      else
      {
        _log.error(String.format("Job %s with ID %s failed! Tracking URL: %s", getJobName(), jobId, this.getTrackingURL()));
      }
      
      return success;
    }
    catch (Exception e)
    {
      _log.error("Exception: " + e.toString());
      throw new Exception(e);
    }
  }
    
  /**
   * Run the job and wait for it to complete.  Output will be temporarily stored under the staging path.
   * If the job is successful it will be moved to the final location.
   */
  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException
  {
    final Path actualOutputPath = FileOutputFormat.getOutputPath(this);
    final Path stagedPath = new Path(String.format("%s/%s/staged", _stagingPrefix, System.currentTimeMillis()));

    FileOutputFormat.setOutputPath(
      this,
      stagedPath
    );

    final Thread hook = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try 
        {
          killJob();
        }
        catch (IOException e) 
        {
          e.printStackTrace();
        }
      }
    });

    Runtime.getRuntime().addShutdownHook(hook);
    
    final boolean retVal = super.waitForCompletion(verbose);
    Runtime.getRuntime().removeShutdownHook(hook);

    if (retVal) 
    {
      FileSystem fs = actualOutputPath.getFileSystem(getConfiguration());

      fs.mkdirs(actualOutputPath);

      _log.info(String.format("Deleting data at old path[%s]", actualOutputPath));
      fs.delete(actualOutputPath, true);

      _log.info(String.format("Moving from staged path[%s] to final resting place[%s]", stagedPath, actualOutputPath));
      boolean renamed = fs.rename(stagedPath, actualOutputPath);
      
      if (renamed && _writeCounters)
      {
        writeCounters(fs);              
      }
      
      return renamed;
    }
    else 
    {
      FileSystem fs = actualOutputPath.getFileSystem(getConfiguration());
      _log.info(String.format("Job failed, deleting staged path[%s]", stagedPath));
      try
      {
        fs.delete(stagedPath, true);
      }
      catch (IOException e)
      {            
      }
    }

    _log.warn("retVal was false for some reason...");
    return retVal;
  }
    
  /**
   * Gets the class for the caller.
   * 
   * @return caller class
   */
  private static Class<?> getCallersClass()
  {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    boolean foundSelf = false;
    for (StackTraceElement element : stack) 
    {
      if (foundSelf &&
          !StagedOutputJob.class.getName().equals(element.getClassName())) 
      {
        try 
        {
          return Class.forName(element.getClassName());
        }
        catch (ClassNotFoundException e) 
        {
          throw new RuntimeException(e);
        }
      }
      else if (StagedOutputJob.class.getName().equals(element.getClassName()) 
               && "getCallersClass".equals(element.getMethodName())) 
      {
        foundSelf = true;
      }
    }
    return StagedOutputJob.class;
  }
    
  /**
   * Writes Hadoop counters and other task statistics to a file in the file system.
   * 
   * @param fs
   * @throws IOException
   */
  private void writeCounters(final FileSystem fs) throws IOException
  {
    final Path actualOutputPath = FileOutputFormat.getOutputPath(this);
    
    SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    
    String suffix = timestampFormat.format(new Date());
    
    if (_countersParentPath != null)
    {
      if (!fs.exists(_countersParentPath))
      {
        _log.info("Creating counter parent path " + _countersParentPath);
        fs.mkdirs(_countersParentPath, FsPermission.valueOf("-rwxrwxr-x"));
      }
      // make the name as unique as possible in this case because this may be a directory
      // where other counter files will be dropped
      _countersPath = new Path(_countersParentPath,".counters." + suffix);
    }
    else
    {
      _countersPath = new Path(actualOutputPath,".counters." + suffix);
    }
    
    _log.info(String.format("Writing counters to %s", _countersPath));
    FSDataOutputStream counterStream = fs.create(_countersPath);
    BufferedOutputStream buffer = new BufferedOutputStream(counterStream,256*1024);
    OutputStreamWriter writer = new OutputStreamWriter(buffer);      
    for (String groupName : getCounters().getGroupNames())
    {
      for (Counter counter : getCounters().getGroup(groupName))
      {
        writeAndLog(writer,String.format("%s=%d",counter.getName(),counter.getValue()));
      }
    }
          
    JobID jobID = this.getJobID();
    
    org.apache.hadoop.mapred.JobID oldJobId = new org.apache.hadoop.mapred.JobID(jobID.getJtIdentifier(),jobID.getId());
    
    long minStart = Long.MAX_VALUE;      
    long maxFinish = 0;
    long setupStart = Long.MAX_VALUE;
    long cleanupFinish = 0;
    DescriptiveStatistics mapStats = new DescriptiveStatistics();
    DescriptiveStatistics reduceStats = new DescriptiveStatistics();
    boolean success = true;
    
    JobClient jobClient = new JobClient(this.conf);
    
    Map<String,String> taskIdToType = new HashMap<String,String>();
               
    TaskReport[] setupReports = jobClient.getSetupTaskReports(oldJobId);
    if (setupReports.length > 0)
    {
      _log.info("Processing setup reports");
      for (TaskReport report : jobClient.getSetupTaskReports(oldJobId))
      {
        taskIdToType.put(report.getTaskID().toString(),"SETUP");
        if (report.getStartTime() == 0)
        {
          _log.warn("Skipping report with zero start time");
          continue;
        }
        setupStart = Math.min(setupStart, report.getStartTime());
      }
    }
    else
    {
      _log.error("No setup reports");
    }
    
    TaskReport[] mapReports = jobClient.getMapTaskReports(oldJobId);
    if (mapReports.length > 0)
    {
      _log.info("Processing map reports");
      for (TaskReport report : mapReports)
      {
        taskIdToType.put(report.getTaskID().toString(),"MAP");
        if (report.getFinishTime() == 0 || report.getStartTime() == 0)
        {
          _log.warn("Skipping report with zero start or finish time");
          continue;
        }
        minStart = Math.min(minStart, report.getStartTime());
        mapStats.addValue(report.getFinishTime() - report.getStartTime());
      }
    }
    else
    {
      _log.error("No map reports");
    }
    
    TaskReport[] reduceReports = jobClient.getReduceTaskReports(oldJobId);
    if (reduceReports.length > 0)
    {
      _log.info("Processing reduce reports");
      for (TaskReport report : reduceReports)
      {      
        taskIdToType.put(report.getTaskID().toString(),"REDUCE");
        if (report.getFinishTime() == 0 || report.getStartTime() == 0)
        {
          _log.warn("Skipping report with zero start or finish time");
          continue;
        }
        maxFinish = Math.max(maxFinish, report.getFinishTime());
        reduceStats.addValue(report.getFinishTime() - report.getStartTime());
      }
    }
    else
    {
      _log.error("No reduce reports");
    }
    
    TaskReport[] cleanupReports = jobClient.getCleanupTaskReports(oldJobId);
    if (cleanupReports.length > 0)
    {
      _log.info("Processing cleanup reports");
      for (TaskReport report : cleanupReports)
      {
        taskIdToType.put(report.getTaskID().toString(),"CLEANUP");
        if (report.getFinishTime() == 0)
        {
          _log.warn("Skipping report with finish time of zero");
          continue;
        }
        cleanupFinish = Math.max(cleanupFinish, report.getFinishTime());
      }
    }
    else
    {
      _log.error("No cleanup reports");
    }
      
    if (minStart == Long.MAX_VALUE)
    {
      _log.error("Could not determine map-reduce start time");
      success = false;
    }      
    if (maxFinish == 0)
    {
      _log.error("Could not determine map-reduce finish time");
      success = false;
    }
    
    if (setupStart == Long.MAX_VALUE)
    {
      _log.error("Could not determine setup start time");
      success = false;
    }      
    if (cleanupFinish == 0)
    {
      _log.error("Could not determine cleanup finish time");
      success = false;
    }     
    
    // Collect statistics on successful/failed/killed task attempts, categorized by setup/map/reduce/cleanup.
    // Unfortunately the job client doesn't have an easier way to get these statistics.
    Map<String,Integer> attemptStats = new HashMap<String,Integer>();
    _log.info("Processing task attempts");            
    for (TaskCompletionEvent event : getTaskCompletionEvents(jobClient,oldJobId))
    {
      String type = taskIdToType.get(event.getTaskAttemptId().getTaskID().toString());
      String status = event.getTaskStatus().toString();
      
      String key = String.format("%s_%s_ATTEMPTS",status,type);
      if (!attemptStats.containsKey(key))
      {
        attemptStats.put(key, 0);
      }
      attemptStats.put(key, attemptStats.get(key) + 1);
    }
              
    if (success)
    {
      writeAndLog(writer,String.format("SETUP_START_TIME_MS=%d",setupStart));
      writeAndLog(writer,String.format("CLEANUP_FINISH_TIME_MS=%d",cleanupFinish));
      writeAndLog(writer,String.format("COMPLETE_WALL_CLOCK_TIME_MS=%d",cleanupFinish - setupStart));
      
      writeAndLog(writer,String.format("MAP_REDUCE_START_TIME_MS=%d",minStart));
      writeAndLog(writer,String.format("MAP_REDUCE_FINISH_TIME_MS=%d",maxFinish));
      writeAndLog(writer,String.format("MAP_REDUCE_WALL_CLOCK_TIME_MS=%d",maxFinish - minStart));
      
      writeAndLog(writer,String.format("MAP_TOTAL_TASKS=%d",(long)mapStats.getN()));
      writeAndLog(writer,String.format("MAP_MAX_TIME_MS=%d",(long)mapStats.getMax()));
      writeAndLog(writer,String.format("MAP_MIN_TIME_MS=%d",(long)mapStats.getMin()));
      writeAndLog(writer,String.format("MAP_AVG_TIME_MS=%d",(long)mapStats.getMean()));
      writeAndLog(writer,String.format("MAP_STD_TIME_MS=%d",(long)mapStats.getStandardDeviation()));
      writeAndLog(writer,String.format("MAP_SUM_TIME_MS=%d",(long)mapStats.getSum()));
      
      writeAndLog(writer,String.format("REDUCE_TOTAL_TASKS=%d",(long)reduceStats.getN()));
      writeAndLog(writer,String.format("REDUCE_MAX_TIME_MS=%d",(long)reduceStats.getMax()));
      writeAndLog(writer,String.format("REDUCE_MIN_TIME_MS=%d",(long)reduceStats.getMin()));
      writeAndLog(writer,String.format("REDUCE_AVG_TIME_MS=%d",(long)reduceStats.getMean()));
      writeAndLog(writer,String.format("REDUCE_STD_TIME_MS=%d",(long)reduceStats.getStandardDeviation()));
      writeAndLog(writer,String.format("REDUCE_SUM_TIME_MS=%d",(long)reduceStats.getSum()));
      
      writeAndLog(writer,String.format("MAP_REDUCE_SUM_TIME_MS=%d",(long)mapStats.getSum() + (long)reduceStats.getSum()));
      
      for (Map.Entry<String, Integer> attemptStat : attemptStats.entrySet())
      {
        writeAndLog(writer,String.format("%s=%d",attemptStat.getKey(),attemptStat.getValue()));
      }
    }
    
    writer.close();
    buffer.close();
    counterStream.close();
  }
    
  /**
   * Get all task completion events for a particular job.
   * 
   * @param jobClient job client
   * @param jobId job ID
   * @return task completion events
   * @throws IOException
   */
  private List<TaskCompletionEvent> getTaskCompletionEvents(JobClient jobClient, org.apache.hadoop.mapred.JobID jobId) throws IOException
  {
    List<TaskCompletionEvent> events = new ArrayList<TaskCompletionEvent>();
    
    // Tries to use reflection to get access to the getTaskCompletionEvents method from the private jobSubmitClient field.
    // This method has a parameter for the size, which defaults to 10 for the top level methods and can therefore be extremely slow
    // if the goal is to get all events.
    
    Method getTaskCompletionEventsMethod = null;
    Object jobSubmitClient = null;
    
    try
    {
      Field f = JobClient.class.getDeclaredField("jobSubmitClient");
      f.setAccessible(true);
      jobSubmitClient = f.get(jobClient);       
      
      if (jobSubmitClient != null)
      { 
        getTaskCompletionEventsMethod = jobSubmitClient.getClass().getDeclaredMethod("getTaskCompletionEvents", org.apache.hadoop.mapred.JobID.class,int.class,int.class);
        getTaskCompletionEventsMethod.setAccessible(true);
      }
    }
    catch (NoSuchMethodException e)
    {
    }
    catch (SecurityException e)
    {
    }
    catch (NoSuchFieldException e)
    {
    }
    catch (IllegalArgumentException e)
    {
    }
    catch (IllegalAccessException e)
    {       
    }
    
    if (getTaskCompletionEventsMethod != null)
    {
      _log.info("Will call getTaskCompletionEvents via reflection since it's faster");
    }
    else
    {
      _log.info("Will call getTaskCompletionEvents via the slow method");
    }
    
    int index = 0;
    while(true)
    {
      TaskCompletionEvent[] currentEvents;
      if (getTaskCompletionEventsMethod != null)
      {
        try
        {
          // grab events, 250 at a time, which is faster than the other method which defaults to 10 at a time (with no override ability)
          currentEvents = (TaskCompletionEvent[])getTaskCompletionEventsMethod.invoke(jobSubmitClient, jobId, index, 250);
        }
        catch (IllegalArgumentException e)
        {
          _log.error("Failed to call getTaskCompletionEventsMethod via reflection, switching to slow direct method", e);
          getTaskCompletionEventsMethod = null;
          continue;
        }
        catch (IllegalAccessException e)
        {
          _log.error("Failed to call getTaskCompletionEventsMethod via reflection, switching to slow direct method", e);
          getTaskCompletionEventsMethod = null;
          continue;
        }
        catch (InvocationTargetException e)
        {
          _log.error("Failed to call getTaskCompletionEventsMethod via reflection, switching to slow direct method", e);
          getTaskCompletionEventsMethod = null;
          continue;
        }
      }
      else
      {
        currentEvents = this.getTaskCompletionEvents(index);
      }        
      if (currentEvents.length == 0) break;
      for (TaskCompletionEvent event : currentEvents)
      {
        events.add(event);
      }
      index += currentEvents.length;
    }
    
    return events;
  }
  
  private void writeAndLog(OutputStreamWriter writer, String line) throws IOException
  {
    writer.append(line);
    writer.append("\n");
    _log.info(line);
  }
  
  static class HiddenFilePathFilter implements PathFilter
  {
    @Override
    public boolean accept(Path path)
    {
      String name = path.getName();
      return ! name.startsWith("_") &&
             ! name.startsWith(".");
    }
  }
}
