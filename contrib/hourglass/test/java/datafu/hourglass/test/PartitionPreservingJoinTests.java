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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
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
import org.codehaus.jackson.node.NullNode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob;
import datafu.hourglass.test.jobs.ImpressionClickPartitionPreservingJob;
import datafu.hourglass.test.util.DailyTrackingWriter;

@Test(groups="pcl")
public class PartitionPreservingJoinTests extends TestBase
{
  private Logger _log = Logger.getLogger(PartitionPreservingJoinTests.class);
  
  private Path _impressionEventPath = new Path("/data/tracking/impressions");
  private Path _clickEventPath = new Path("/data/tracking/clicks");
  private Path _outputPath = new Path("/output");
  
  private static final Schema IMPRESSION_SCHEMA;
  private static final Schema CLICK_SCHEMA;
  
  private Properties _props;
  private GenericRecord _impressionRecord;
  private GenericRecord _clickRecord;
  private DailyTrackingWriter _impressionWriter;
  private DailyTrackingWriter _clickWriter;
  
  private int _maxIterations;
  private int _maxDaysToProcess;
  private int _retentionCount;
  private String _startDate;
  private String _endDate;
  private Integer _numDays;
    
  static
  {
    // Create two schemas having the same name, but different types (int vs long).  The fields using this schema are not used.  This simply
    // tests that we can join two conflicting schemas.
    Schema intSchema = Schema.createRecord("Dummy", null, "datafu.hourglass", false);
    intSchema.setFields(Arrays.asList(
        new Field("dummy",Schema.create(Type.INT), null,null)));    
    Schema longSchema = Schema.createRecord("Dummy", null, "datafu.hourglass", false);
    longSchema.setFields(Arrays.asList(
        new Field("dummy",Schema.create(Type.LONG), null,null)));
    
    IMPRESSION_SCHEMA = Schemas.createRecordSchema(PartitionPreservingJoinTests.class, "Impression",
                                               new Field("id", Schema.create(Type.LONG), "ID", null),
                                               new Field("dummy", Schema.createUnion(Arrays.asList(Schema.create(Type.NULL),intSchema)), null, NullNode.instance));
    
    CLICK_SCHEMA = Schemas.createRecordSchema(PartitionPreservingJoinTests.class, "Click",
                                               new Field("id", Schema.create(Type.LONG), "ID", null),
                                               new Field("dummy", Schema.createUnion(Arrays.asList(Schema.create(Type.NULL),longSchema)), null, NullNode.instance));
  }
  
  public PartitionPreservingJoinTests() throws IOException
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
    getFileSystem().delete(_impressionEventPath, true); 
    getFileSystem().delete(_clickEventPath, true);
    getFileSystem().delete(_outputPath, true);
    getFileSystem().mkdirs(_impressionEventPath);
    getFileSystem().mkdirs(_clickEventPath);
    getFileSystem().mkdirs(_outputPath);
    
    _maxIterations = 20;
    _maxDaysToProcess = 365;
    _retentionCount = 3;
    _numDays = null;
    _startDate = null;
    _endDate = null;
     
    _impressionRecord = new GenericData.Record(IMPRESSION_SCHEMA);
    _clickRecord = new GenericData.Record(CLICK_SCHEMA);
    
    _impressionWriter = new DailyTrackingWriter(_impressionEventPath,IMPRESSION_SCHEMA,getFileSystem());
    _clickWriter = new DailyTrackingWriter(_clickEventPath,CLICK_SCHEMA,getFileSystem());
  }
  
  @Test
  public void joinTest() throws IOException, InterruptedException, ClassNotFoundException
  {    
    startImpressions(2013, 3, 15);
    startClicks(2013, 3, 15);
    impression(1); click(1);
    impression(2); click(2); click(2);
    impression(3); impression(3);
    impression(4); click(4);
    stopImpressions();
    stopClicks();
    
    startImpressions(2013, 3, 16);
    startClicks(2013, 3, 16);
    impression(5); click(5);
    impression(6); impression(6); click(6);
    impression(7);
    impression(7);
    stopImpressions();
    stopClicks();
    
    startImpressions(2013, 3, 17);
    startClicks(2013, 3, 17);
    impression(8); click(8); click(8); click(8);
    impression(9);
    impression(10); click(10); click(10);
    stopImpressions();
    stopClicks();
    
    runJob();
    
    checkOutputFolderCount(3);
        
    HashMap<Long,ImpressionClick> counts;
    
    counts = loadOutputCounts("20130315");    
    checkSize(counts,4);    
    checkIdCount(counts,1,1,1);
    checkIdCount(counts,2,1,2);
    checkIdCount(counts,3,2,0);
    checkIdCount(counts,4,1,1);
    
    counts = loadOutputCounts("20130316");    
    checkSize(counts,3);    
    checkIdCount(counts,5,1,1);
    checkIdCount(counts,6,2,1);
    checkIdCount(counts,7,2,0);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,3);    
    checkIdCount(counts,8,1,3);
    checkIdCount(counts,9,1,0);
    checkIdCount(counts,10,1,2);
  }
  
  @Test
  public void joinClicksMissingTest() throws IOException, InterruptedException, ClassNotFoundException
  {    
    startImpressions(2013, 3, 15);
    startClicks(2013, 3, 15);
    impression(1); click(1);
    impression(2); click(2); click(2);
    impression(3); impression(3);
    impression(4); click(4);
    stopImpressions();
    stopClicks();
    
    startImpressions(2013, 3, 16);
    startClicks(2013, 3, 16);
    impression(5); click(5);
    impression(6); impression(6); click(6);
    impression(7);
    impression(7);
    stopImpressions();
    stopClicks();
    
    startImpressions(2013, 3, 17);
    startClicks(2013, 3, 17);
    impression(8);
    impression(9);
    impression(10);
    stopImpressions();
    stopClicks();
    
    runJob();
    
    checkOutputFolderCount(3);
        
    HashMap<Long,ImpressionClick> counts;
    
    counts = loadOutputCounts("20130315");    
    checkSize(counts,4);    
    checkIdCount(counts,1,1,1);
    checkIdCount(counts,2,1,2);
    checkIdCount(counts,3,2,0);
    checkIdCount(counts,4,1,1);
    
    counts = loadOutputCounts("20130316");    
    checkSize(counts,3);    
    checkIdCount(counts,5,1,1);
    checkIdCount(counts,6,2,1);
    checkIdCount(counts,7,2,0);
    
    counts = loadOutputCounts("20130317");    
    checkSize(counts,3);    
    checkIdCount(counts,8,1,0);
    checkIdCount(counts,9,1,0);
    checkIdCount(counts,10,1,0);
  }
  
  private AbstractPartitionPreservingIncrementalJob runJob() throws IOException, InterruptedException, ClassNotFoundException
  {
    _props = newTestProperties();
    _props.setProperty("max.iterations", Integer.toString(_maxIterations));
    _props.setProperty("max.days.to.process", Integer.toString(_maxDaysToProcess));
    _props.setProperty("retention.count", Integer.toString(_retentionCount));
    _props.setProperty("input.path.impressions", _impressionEventPath.toString());
    _props.setProperty("input.path.clicks", _clickEventPath.toString());
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
    
    AbstractPartitionPreservingIncrementalJob c = new ImpressionClickPartitionPreservingJob("clicks",_props);    
    c.run();
    
    return c;
  }
    
  private void impression(long id) throws IOException
  {
    _impressionRecord.put("id", id);    
    _impressionWriter.append(_impressionRecord);
  }
    
  private void click(long id) throws IOException
  {
    _clickRecord.put("id", id);    
    _clickWriter.append(_impressionRecord);
  }
  
  private void startImpressions(int year, int month, int day) throws IOException
  {
    _impressionWriter.open(year, month, day);
  }
  
  private void stopImpressions() throws IOException
  {
    _impressionWriter.close();
  }
  
  private void startClicks(int year, int month, int day) throws IOException
  {
    _clickWriter.open(year, month, day);
  }
  
  private void stopClicks() throws IOException
  {
    _clickWriter.close();
  }
  
  private void checkOutputFolderCount(int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(),expectedCount);
  }
  
  private void checkSize(HashMap<?,?> counts, int expectedSize)
  {
    Assert.assertEquals(counts.size(), expectedSize);
  }
  
  private void checkIdCount(HashMap<Long,ImpressionClick> counts, long id, int impressions, int clicks)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).impressions, impressions);
    Assert.assertEquals(counts.get(id).clicks, clicks);
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
  
  class ImpressionClick
  {
    public int clicks;
    public int impressions;
  }
  
  private HashMap<Long,ImpressionClick> loadOutputCounts(String timestamp) throws IOException
  {
    HashMap<Long,ImpressionClick> counts = new HashMap<Long,ImpressionClick>();
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
          Integer impressions = (Integer)((GenericRecord)r.get("value")).get("impressions");    
          Integer clicks = (Integer)((GenericRecord)r.get("value")).get("clicks");         
          Assert.assertFalse(counts.containsKey(memberId));
          ImpressionClick data = new ImpressionClick();
          data.clicks = clicks;
          data.impressions = impressions;
          counts.put(memberId, data);
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
