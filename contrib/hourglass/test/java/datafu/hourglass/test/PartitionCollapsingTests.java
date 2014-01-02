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
import java.util.ArrayList;
import java.util.Collections;
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
import datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob.Report;
import datafu.hourglass.test.jobs.SimplePartitionCollapsingCountJob2;
import datafu.hourglass.test.jobs.counting.PartitionCollapsingIncrementalCountJob;
import datafu.hourglass.test.util.DailyTrackingWriter;

@Test(groups="pcl")
public class PartitionCollapsingTests extends TestBase
{
  private Logger _log = Logger.getLogger(PartitionCollapsingTests.class);
  
  private Path _inputPath = new Path("/input");
  private Path _outputPath = new Path("/output");
  
  private static final Schema EVENT_SCHEMA;
  
  private Properties _props;
  private GenericRecord _record;
  private DailyTrackingWriter _eventWriter;
  
  private int _maxIterations;
  private int _maxDaysToProcess;
  private int _retentionCount;
  private boolean _reusePreviousOutput;
  private String _startDate;
  private String _endDate;
  private Integer _numDays;
    
  static
  {    
    EVENT_SCHEMA = Schemas.createRecordSchema(PartitionCollapsingTests.class, "Event",
                                              new Field("id", Schema.create(Type.LONG), "ID", null));
  }
  
  public PartitionCollapsingTests() throws IOException
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
    _reusePreviousOutput = false;
           
    _record = new GenericData.Record(EVENT_SCHEMA);    
    _eventWriter = new DailyTrackingWriter(_inputPath,EVENT_SCHEMA,getFileSystem());
  }
  
  @Test
  public void singleDay() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);    
    closeDayForEvent();
            
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130315");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,4);
    checkIdCount(counts,3,7);
  }
  
  @Test
  public void multipleDays() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1,1,1);        
    storeIds(2,2);
    storeIds(3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2);
    storeIds(3,3);    
    closeDayForEvent();
            
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130317");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,13);
    checkIdCount(counts,2,14);
    checkIdCount(counts,3,15);
  }
  
  @Test
  public void multipleIterations() throws IOException, InterruptedException, ClassNotFoundException
  {          
    // has to run 3 times since can only process 1 day at a time
    _maxIterations = 3;
    _maxDaysToProcess = 1;
    _reusePreviousOutput = true;
    
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1,1,1);        
    storeIds(2,2);
    storeIds(3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2);
    storeIds(3,3);    
    closeDayForEvent();
            
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(3, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
      
      report = reports.get(1);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertNotNull(report.getReusedOutput()); // reuse output
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
      
      report = reports.get(2);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertNotNull(report.getReusedOutput()); // reuse output
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(3);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130315");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,4);
    checkIdCount(counts,3,7);
    
    counts = loadOutputCounts("20130316");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,7);
    checkIdCount(counts,2,6);
    checkIdCount(counts,3,13);
    
    counts = loadOutputCounts("20130317");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,13);
    checkIdCount(counts,2,14);
    checkIdCount(counts,3,15);
  }
  
  @Test
  public void multipleRunsFixedStartReuseOutput() throws IOException, InterruptedException, ClassNotFoundException
  {          
    _reusePreviousOutput = true;
    
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1);        
    storeIds(2,2,2,2);
    storeIds(3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1,1,1);        
    storeIds(2,2);
    storeIds(3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2);
    storeIds(3,3);    
    closeDayForEvent();
                
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130317");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,13);
    checkIdCount(counts,2,14);
    checkIdCount(counts,3,15);
            
    openDayForEvent(2013, 3, 18);        
    storeIds(1,1);        
    storeIds(2,2,2,2,2);
    storeIds(3,3,3,3);    
    storeIds(4,4,4);
    storeIds(5,5);
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 19);        
    storeIds(1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2);
    storeIds(3,3);    
    storeIds(4,4,4,4,4);
    closeDayForEvent();
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertNotNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(2);
    
    // should still exist
    checkOutputExists("20130317");
    
    counts = loadOutputCounts("20130319");
    checkSize(counts,5);    
    checkIdCount(counts,1,21);
    checkIdCount(counts,2,27);
    checkIdCount(counts,3,21);
    checkIdCount(counts,4,8);
    checkIdCount(counts,5,2);
  }
  
  @Test
  public void startDate() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1);        
    storeIds(2,2,2,2);
    storeIds(3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 18);        
    storeIds(1);        
    storeIds(2,2);
    storeIds(3,3,3,3,3);    
    closeDayForEvent();
    
    _startDate = "20130317";
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130318");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,6);
    checkIdCount(counts,3,6);
  }
  
  @Test
  public void endDate() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1);        
    storeIds(2,2,2,2);
    storeIds(3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1);        
    storeIds(2,2);
    storeIds(3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 18);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    _endDate = "20130316";
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130316");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,6);
    checkIdCount(counts,3,6);
  }
  
  @Test
  public void startAndEndDate() throws IOException, InterruptedException, ClassNotFoundException
  {          
    openDayForEvent(2013, 3, 11);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 12);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 13);        
    storeIds(1,1);        
    storeIds(2,2,2,2);
    storeIds(3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 14);        
    storeIds(1);        
    storeIds(2,2);
    storeIds(3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1,1,1,1,1,1,1,1,1);        
    storeIds(2,2,2,2,2,2,2,2,2,2,2,2);
    storeIds(3,3,3,3,3,3,3,3,3,3,3,3,3,3);    
    closeDayForEvent();
  
    _startDate = "20130313";
    _endDate = "20130314";
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130314");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,3);
    checkIdCount(counts,2,6);
    checkIdCount(counts,3,4);
  }
  
  @Test
  public void multipleRunsFixedWindowNoReuse() throws IOException, InterruptedException, ClassNotFoundException
  {              
    _numDays = 3;
    
    openDay(2013, 3, 15);     
    storeCount(1,15);
    storeCount(2,25);
    storeCount(3,7);
    storeCount(10,3);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeCount(1,4);
    storeCount(2,9);
    storeCount(3,12);
    storeCount(4,3);
    closeDay();
    
    openDay(2013, 3, 17);
    storeCount(3,5);
    storeCount(5,13);
    closeDay();
            
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130317");    
    checkSize(counts,6);    
    checkIdCount(counts,1,19);
    checkIdCount(counts,2,34);
    checkIdCount(counts,3,24);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,13);
    checkIdCount(counts,10,3);
    
    openDay(2013, 3, 18);
    storeCount(3,9);
    storeCount(5,24);
    storeCount(6,7);
    closeDay();
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(2);
    
    counts = loadOutputCounts("20130318");    
    checkSize(counts,6);    
    checkIdCount(counts,1,4);
    checkIdCount(counts,2,9);
    checkIdCount(counts,3,26);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,37);
    checkIdCount(counts,6,7);
  }
  
  @Test
  public void severalRunsFixedWindowReuseOutput() throws IOException, InterruptedException, ClassNotFoundException
  {
    HashMap<Long,Long> counts;
    
    _reusePreviousOutput = true;
    _numDays = 10;
    
    for (int day=1; day<=10; day++)
    {
      openDay(2013, 3, day);     
      storeCount(day,1); // one id per day
      closeDay();
    }
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),10);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    counts = loadOutputCounts("20130310");    
    checkSize(counts,10);    
    for (int day=1; day<=10; day++)
    {
      checkIdCount(counts,day,1);
    }
    
    for (int day=11; day<=13; day++)
    {
      openDay(2013, 3, day);     
      storeCount(day,1); // one id per day
      closeDay();
    }
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),3);
      Assert.assertNotNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),3);
    }
    
    counts = loadOutputCounts("20130313");    
    checkSize(counts,10);    
    for (int day=4; day<=13; day++)
    {
      checkIdCount(counts,day,1);
    }
    
    for (int day=14; day<=15; day++)
    {
      openDay(2013, 3, day);
      storeCount(day,1); // one id per day
      closeDay();
    }
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),2);
      Assert.assertNotNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),2);
    }
    
    counts = loadOutputCounts("20130315");    
    checkSize(counts,10);    
    for (int day=6; day<=15; day++)
    {
      checkIdCount(counts,day,1);
    }
  }
  
  @Test
  public void twoRunsFixedWindowReuseOutput() throws IOException, InterruptedException, ClassNotFoundException
  {          
    _reusePreviousOutput = true;
    _numDays = 6;
    
    // this day should be ignored
    openDay(2013, 3, 14);     
    storeCount(1,5);
    storeCount(2,17);
    storeCount(3,9);
    storeCount(99,9);
    closeDay();
    
    // first day consumed
    openDay(2013, 3, 15);     
    storeCount(1,15);
    storeCount(2,25);
    storeCount(3,7);
    storeCount(100,1);
    storeCount(101,1);
    storeCount(102,10);
    closeDay();
    
    openDay(2013, 3, 16);        
    storeCount(1,4);
    storeCount(2,9);
    storeCount(3,12);
    storeCount(4,3);
    closeDay();
    
    openDay(2013, 3, 17);
    storeCount(3,5);
    storeCount(5,13);
    closeDay();

    openDay(2013, 3, 18);
    storeCount(3,9);
    storeCount(5,24);
    storeCount(6,7);
    closeDay();
    
    openDay(2013, 3, 19);
    storeCount(3,9);
    storeCount(5,24);
    storeCount(6,1);
    storeCount(7,4);
    closeDay();
    
    // last day consumed
    openDay(2013, 3, 20);
    storeCount(1,1);
    storeCount(3,10);
    storeCount(5,1);
    storeCount(6,7);
    storeCount(8,9);
    closeDay();
            
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),6);
      Assert.assertNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),0);
    }
    
    checkOutputFolderCount(1);
    
    HashMap<Long,Long> counts = loadOutputCounts("20130320");    
    checkSize(counts,11);    
    checkIdCount(counts,1,20);
    checkIdCount(counts,2,34);
    checkIdCount(counts,3,52);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,62);
    checkIdCount(counts,6,15);
    checkIdCount(counts,7,4);
    checkIdCount(counts,8,9);
    checkIdCount(counts,100,1);
    checkIdCount(counts,101,1);
    checkIdCount(counts,102,10);
    
    // next day to be added, 15th will be dropped
    openDay(2013, 3, 21);
    storeCount(1,9);
    storeCount(3,15);
    storeCount(6,9);
    storeCount(8,20);
    storeCount(9,20); // new key
    storeCount(100,1); // replaces dropped value so no change
    // 101 is missing
    storeCount(102,5); // not as large as dropped value
    closeDay();
    
    // the job should subtract the 15th and add the 21st
    
    {
      PartitionCollapsingIncrementalCountJob job = runJob();
      List<Report> reports = job.getReports();
      Assert.assertEquals(1, reports.size());
      Report report = reports.get(0);
      Assert.assertEquals(report.getInputFiles().size(),1);
      Assert.assertNotNull(report.getReusedOutput());
      Assert.assertNotNull(report.getOutputPath());
      Assert.assertEquals(report.getOldInputFiles().size(),1);
    }
    
    checkOutputFolderCount(2);
    
    counts = loadOutputCounts("20130321");    
    checkSize(counts,11);    
    checkIdCount(counts,1,14);
    checkIdCount(counts,2,9);
    checkIdCount(counts,3,60);
    checkIdCount(counts,4,3);
    checkIdCount(counts,5,62);
    checkIdCount(counts,6,24);
    checkIdCount(counts,7,4);
    checkIdCount(counts,8,29);
    checkIdCount(counts,9,20);
    checkIdCount(counts,100,1);
    checkIdCount(counts,102,5);
  }
  
  private void storeCount(long id, long count) throws IOException
  {
    for (int i=0; i<count; i++)
    {
      storeId(id);
    }
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
    _eventWriter.append(_record);
  }
  
  private void checkOutputFolderCount(int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(),expectedCount,"Found: " + listOutputFolders());
  }
  
  private int countOutputFolders() throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.listStatus(_outputPath,PathUtils.nonHiddenPathFilter).length;
  }
  
  private String listOutputFolders() throws IOException
  {
    StringBuilder sb = new StringBuilder();
    for (FileStatus stat : getFileSystem().listStatus(_outputPath,PathUtils.nonHiddenPathFilter))
    {
      sb.append(stat.getPath().getName());
      sb.append(",");
    }
    return sb.toString();
  }
  
  private void checkSize(HashMap<Long,Long> counts, int expectedSize)
  {
    if (counts.size() != expectedSize)
    {
      StringBuilder sb = new StringBuilder("Expected count " + expectedSize + " does not match actual " + counts.size() + ", contents:\n");
      List<Long> keys = new ArrayList<Long>(counts.keySet());
      Collections.sort(keys);
      for (Long k : keys)
      {
        sb.append(k.toString() + " => " + counts.get(k).toString() + "\n");
      }
      Assert.fail(sb.toString());
    }
  }
  
  private void checkIdCount(HashMap<Long,Long> counts, long id, long count)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).longValue(), count);
  }
      
  private void openDay(int year, int month, int day) throws IOException
  {
    openDayForEvent(year,month,day);
  }
  
  private void closeDay() throws IOException
  {
    closeDayForEvent();
  }
  
  private void openDayForEvent(int year, int month, int day) throws IOException
  {
    _eventWriter.open(year, month, day);
  }
  
  private void closeDayForEvent() throws IOException
  {
    _eventWriter.close();
  }
  
  private void checkOutputExists(String timestamp) throws IOException
  {
    FileSystem fs = getFileSystem();
    Assert.assertTrue(fs.exists(new Path(_outputPath, timestamp)));
  }
  
  private SimplePartitionCollapsingCountJob2 runJob() throws IOException, InterruptedException, ClassNotFoundException
  {
    _props = newTestProperties();
    _props.setProperty("max.iterations", Integer.toString(_maxIterations));
    _props.setProperty("max.days.to.process", Integer.toString(_maxDaysToProcess));
    _props.setProperty("retention.count", Integer.toString(_retentionCount));
    _props.setProperty("use.combiner", "true");
    _props.setProperty("num.reducers.bytes.per.reducer", "64000");
    _props.setProperty("num.reducers.previous.bytes.per.reducer", "256000");
    _props.setProperty("input.path", _inputPath.toString());
    _props.setProperty("output.path", _outputPath.toString());
    _props.setProperty("reuse.previous.output", Boolean.toString(_reusePreviousOutput));
        
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
    
    SimplePartitionCollapsingCountJob2 c = new SimplePartitionCollapsingCountJob2("counts",_props);  
    c.run();
    
    return c;
  }
  
  private HashMap<Long,Long> loadOutputCounts(String timestamp) throws IOException
  {
    HashMap<Long,Long> counts = new HashMap<Long,Long>();
    FileSystem fs = getFileSystem();
    Assert.assertTrue(fs.exists(new Path(_outputPath, timestamp)));
    for (FileStatus stat : fs.globStatus(new Path(_outputPath,timestamp + "/*.avro")))
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
