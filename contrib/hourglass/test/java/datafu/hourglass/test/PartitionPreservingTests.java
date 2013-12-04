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

package datafu.hourglass.test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob.Report;
import datafu.hourglass.test.jobs.SimplePartitionPreservingCountJob;
import datafu.hourglass.test.util.DailyTrackingWriter;

@Test(groups="pcl")
public class PartitionPreservingTests extends TestBase
{
  private Logger _log = Logger.getLogger(PartitionPreservingTests.class);
  
  private Path _inputPath = new Path("/data/tracking/SimpleEvent");
  private Path _outputPath = new Path("/output");
  
  private static final Schema EVENT_SCHEMA;
  
  private Properties _props;
  private GenericRecord _record;
  private DailyTrackingWriter _writer;
  
  private int _maxIterations;
  private int _maxDaysToProcess;
  private int _retentionCount;
  private String _startDate;
  private String _endDate;
  private Integer _numDays;
    
  static
  {
    EVENT_SCHEMA = Schemas.createRecordSchema(PartitionPreservingTests.class, "Event",
                                              new Field("id", Schema.create(Type.LONG), "ID", null));
  }
  
  public PartitionPreservingTests() throws IOException
  {
    super();
  }
  
  @BeforeClass
  public void beforeClass() throws Exception
  {
    super.beforeClass();
  }
  
  @AfterClass
  public void afterClass() throws Exception
  {
    super.afterClass();
  }
  
  @BeforeMethod
  public void beforeMethod(Method method) throws IOException
  {
    _log.info("*** Running " + method.getName());
    
    _log.info("*** Cleaning input and output paths");  
    getFileSystem().delete(_inputPath, true);
    getFileSystem().delete(_outputPath, true);
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().mkdirs(_outputPath);
    
    _maxIterations = 20;
    _maxDaysToProcess = 365;
    _retentionCount = 3;
    _numDays = null;
    _startDate = null;
    _endDate = null;
     
    _record = new GenericData.Record(EVENT_SCHEMA);
    
    _writer = new DailyTrackingWriter(_inputPath,EVENT_SCHEMA,getFileSystem());
  }
  
  @Test
  public void numDays() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 2;
    
    // too old, will be ignored
    openDay(2013, 3, 15);     
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeIds(1,1,1,1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5);
    closeDay();
    
    openDay(2013, 3, 17);             
    storeIds(2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3);  
    storeIds(5,5,5,5);
    storeIds(6,6,6);
    closeDay();
            
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(reports.size(), 1);
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertEquals(report.getOutputFiles().size(),2);
    }
    
    checkOutputFolderCount(2);
        
    HashMap<Long,Long> counts = loadOutputCounts("20130316");    
    checkSize(counts,5);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,4);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,1);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,4);    
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,10);
    checkIdCount(counts,5,4);
    checkIdCount(counts,6,3);
  }
  
  @Test
  public void startDate() throws IOException, InterruptedException, ClassNotFoundException
  {
    _startDate = "20130315";
    _numDays = 2;
    
    // outside time range
    openDay(2013, 3, 14);     
    storeIds(1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 15);     
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeIds(1,1,1,1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5);
    closeDay();

    // outside time range
    openDay(2013, 3, 17);             
    storeIds(2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3);  
    storeIds(5,5,5,5);
    storeIds(6,6,6);
    closeDay();
            
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(reports.size(), 1);
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertEquals(report.getOutputFiles().size(),2);
    }
    
    checkOutputFolderCount(2);
        
    HashMap<Long,Long> counts = loadOutputCounts("20130315");    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,4);
    checkIdCount(counts,3,7);
    
    counts = loadOutputCounts("20130316");    
    checkSize(counts,5);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,4);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,1);
  }
  
  @Test
  public void endDate() throws IOException, InterruptedException, ClassNotFoundException
  {
    _endDate = "20130317";
    _numDays = 2;
    
    // outside time range
    openDay(2013, 3, 15);     
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeIds(1,1,1,1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5);
    closeDay();
    
    openDay(2013, 3, 17);             
    storeIds(2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3);  
    storeIds(5,5,5,5);
    storeIds(6,6,6);
    closeDay();
    
    // outside time range
    openDay(2013, 3, 18);             
    storeIds(2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3);  
    storeIds(5,5,5);
    storeIds(6,6,6);
    closeDay();
            
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(reports.size(), 1);
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertEquals(report.getOutputFiles().size(),2);
    }
    
    checkOutputFolderCount(2);
            
    HashMap<Long,Long> counts = loadOutputCounts("20130316");    
    checkSize(counts,5);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,4);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,1);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,4);    
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,10);
    checkIdCount(counts,5,4);
    checkIdCount(counts,6,3);
  }
  
  @Test
  public void startAndEndDate() throws IOException, InterruptedException, ClassNotFoundException
  {
    _startDate = "20130316";
    _endDate = "20130317";
    
    // outside time range
    openDay(2013, 3, 15);     
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeIds(1,1,1,1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5);
    closeDay();
    
    openDay(2013, 3, 17);             
    storeIds(2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3);  
    storeIds(5,5,5,5);
    storeIds(6,6,6);
    closeDay();
    
    // outside time range
    openDay(2013, 3, 18);             
    storeIds(2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3);  
    storeIds(5,5,5);
    storeIds(6,6,6);
    closeDay();
            
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(reports.size(), 1);
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertEquals(report.getOutputFiles().size(),2);
    }
    
    checkOutputFolderCount(2);
            
    HashMap<Long,Long> counts = loadOutputCounts("20130316");    
    checkSize(counts,5);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,4);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,1);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,4);    
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,10);
    checkIdCount(counts,5,4);
    checkIdCount(counts,6,3);
  }
  
  @Test
  public void multipleRuns() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDay(2013, 3, 15);     
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeIds(1,1,1,1,1);        
    storeIds(2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5);
    closeDay();
    
    openDay(2013, 3, 17);             
    storeIds(2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3);  
    storeIds(5,5,5,5);
    storeIds(6,6,6);
    closeDay();
                
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(reports.size(), 1);
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertEquals(report.getOutputFiles().size(),3);
    }
    
    checkOutputFolderCount(3);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130315");    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,4);
    checkIdCount(counts,3,7);
    
    counts = loadOutputCounts("20130316");    
    checkSize(counts,5);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,4);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,1);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,4);    
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,10);
    checkIdCount(counts,5,4);
    checkIdCount(counts,6,3);
        
    // new day
    openDay(2013, 3, 18);             
    storeIds(2,2);
    storeIds(3,3,3,3,3,3);  
    storeIds(5,5);
    storeIds(7,7,7);
    closeDay();
    
    {
      SimplePartitionPreservingCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertEquals(report.getOutputFiles().size(),1);
    }
    
    checkOutputFolderCount(3); // only 3 in output due to retention count
    
    counts = loadOutputCounts("20130318");    
    checkSize(counts,4);    
    checkIdCount(counts,2,2);
    checkIdCount(counts,3,6);
    checkIdCount(counts,5,2);
    checkIdCount(counts,7,3);
  }
  
  private SimplePartitionPreservingCountJob runJob() throws IOException, InterruptedException, ClassNotFoundException
  {
    _props = newTestProperties();
    _props.setProperty("max.iterations", Integer.toString(_maxIterations));
    _props.setProperty("max.days.to.process", Integer.toString(_maxDaysToProcess));
    _props.setProperty("retention.count", Integer.toString(_retentionCount));
    _props.setProperty("use.combiner", "true");
    _props.setProperty("input.path", _inputPath.toString());
    _props.setProperty("output.path", _outputPath.toString());
    
    if (_numDays != null && _numDays > 0)
    {
      _props.setProperty("num.days", _numDays.toString());
    }
        
    if (_startDate != null)
    {
      _props.setProperty("start.date", _startDate);
    }
    
    if (_endDate != null)
    {
      _props.setProperty("end.date", _endDate);
    }
    
    SimplePartitionPreservingCountJob c = new SimplePartitionPreservingCountJob("counts",_props); 
    c.run();
    
    return c;
  }
  
  private void storeIds(long... ids) throws IOException
  {
    for (long id : ids)
    {
      storeId(id);
    }
  }
    
  private void storeId(long id) throws IOException
  {
    _record.put("id", id);    
    _writer.append(_record);
  }
  
  private void openDay(int year, int month, int day) throws IOException
  {
    _writer.open(year, month, day);
  }
  
  private void closeDay() throws IOException
  {
    _writer.close();
  }
  
  private void checkOutputFolderCount(int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(),expectedCount);
  }
  
  private void checkSize(HashMap<Long,Long> counts, int expectedSize)
  {
    Assert.assertEquals(counts.size(), expectedSize);
  }
  
  private void checkIdCount(HashMap<Long,Long> counts, long id, long count)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).longValue(), count);
  }
  
  private int countOutputFolders() throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.globStatus(new Path(_outputPath,"*/*/*"),PathUtils.nonHiddenPathFilter).length;
  }
  
  private String getNestedPathFromTimestamp(String timestamp)
  {
    Date date = null;
    try
    {
      date = PathUtils.datedPathFormat.parse(timestamp);
    }
    catch (ParseException e)
    {
      Assert.fail();
    }
    String nestedPath = PathUtils.nestedDatedPathFormat.format(date);
    return nestedPath;
  }
  
  private HashMap<Long,Long> loadOutputCounts(String timestamp) throws IOException
  {
    HashMap<Long,Long> counts = new HashMap<Long,Long>();
    FileSystem fs = getFileSystem();
    String nestedPath = getNestedPathFromTimestamp(timestamp);
    Assert.assertTrue(fs.exists(new Path(_outputPath, nestedPath)));
    for (FileStatus stat : fs.globStatus(new Path(_outputPath,nestedPath + "/*.avro")))
    {
      _log.info(String.format("found: %s (%d bytes)",stat.getPath(),stat.getLen()));
      FSDataInputStream is = fs.open(stat.getPath());
      DatumReader <GenericRecord> reader = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(is, reader);
      
      try
      {
        while (dataFileStream.hasNext())
        {
          GenericRecord r = dataFileStream.next();
          Long memberId = (Long)((GenericRecord)r.get("key")).get("id");
          Long count = (Long)((GenericRecord)r.get("value")).get("count");        
          Assert.assertFalse(counts.containsKey(memberId));
          counts.put(memberId, count);
        }
      }
      finally
      {
        dataFileStream.close();
      }
    }
    return counts;
  }
}
