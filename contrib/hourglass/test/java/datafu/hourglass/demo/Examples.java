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

package datafu.hourglass.demo;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
import datafu.hourglass.test.Schemas;
import datafu.hourglass.test.PartitionCollapsingTests;
import datafu.hourglass.test.TestBase;
import datafu.hourglass.test.util.DailyTrackingWriter;

@Test(groups="pcl")
public class Examples extends TestBase
{
  private Logger _log = Logger.getLogger(PartitionCollapsingTests.class);
    
  private static final Schema EVENT_SCHEMA;
  
  private GenericRecord _record;
  private DailyTrackingWriter _eventWriter;
  
  static
  {    
    EVENT_SCHEMA = Schemas.createRecordSchema(Examples.class, "Event",
                                              new Field("id", Schema.create(Type.LONG), null, null));
    
    System.out.println("Event schema: " + EVENT_SCHEMA.toString(true));
  }
  
  public Examples() throws IOException
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
    getFileSystem().delete(new Path("/data"), true);
    getFileSystem().delete(new Path("/output"), true);
    getFileSystem().mkdirs(new Path("/data"));
    getFileSystem().mkdirs(new Path("/output"));
               
    _record = new GenericData.Record(EVENT_SCHEMA);    
    _eventWriter = new DailyTrackingWriter(new Path("/data/event"),EVENT_SCHEMA,getFileSystem());
  }
  
  @Test
  public void countByMember() throws IOException, InterruptedException, ClassNotFoundException
  {          
    // setup
    openDayForEvent(2013, 3, 15);        
    storeIds(1,1,1);        
    storeIds(2);
    storeIds(3,3);    
    closeDayForEvent();
    
    openDayForEvent(2013, 3, 16);        
    storeIds(1,1);        
    storeIds(2,2);
    storeIds(3);    
    closeDayForEvent();
            
    // run    
    new CountById().run(createJobConf(),"/data/event","/output");
    
    // verify
    
    checkOutputFolderCount(new Path("/output"), 1);
    
    HashMap<Long,Integer> counts = loadOutputCounts(new Path("/output"), "20130316");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,5);
    checkIdCount(counts,2,3);
    checkIdCount(counts,3,3);

    // more data
    openDayForEvent(2013, 3, 17);        
    storeIds(1,1);        
    storeIds(2,2,2);
    storeIds(3,3);    
    closeDayForEvent();
    
    // run    
    new CountById().run(createJobConf(),"/data/event","/output");
    
    counts = loadOutputCounts(new Path("/output"), "20130317");
    
    checkSize(counts,3);    
    checkIdCount(counts,1,7);
    checkIdCount(counts,2,6);
    checkIdCount(counts,3,5);
  }
  
  @Test
  public void estimateNumMembers() throws IOException, InterruptedException, ClassNotFoundException
  {     
    openDayForEvent(2013, 3, 1);  
    // lots of members logged in this day
    for (int i=1; i<=10000; i++) 
    {
      storeIds(i);   
    } 
    closeDayForEvent();
    
    // but only a handful logged in the remaining days
    for (int i=2; i<=30; i++) 
    {
      openDayForEvent(2013, 3, i);        
      storeIds(1,2,3,4,5);    
      closeDayForEvent();
    }
        
    // run    
    new EstimateCardinality().run(createJobConf(),"/data/event","/output/daily","/output/summary",30);
    
    // verify    
    checkIntermediateFolderCount(new Path("/output/daily"), 30);
    checkOutputFolderCount(new Path("/output/summary"), 1);
    Assert.assertTrue(Math.abs(10000L - loadMemberCount(new Path("/output/summary"),"20130330").longValue())/10000.0 < 0.005);

    // more data
    openDayForEvent(2013, 3, 31);        
    storeIds(6,7,8,9,10);
    closeDayForEvent();
    
    // run    
    new EstimateCardinality().run(createJobConf(),"/data/event","/output/daily","/output/summary",30);
    
    // verify    
    checkIntermediateFolderCount(new Path("/output/daily"), 31);
    checkOutputFolderCount(new Path("/output/summary"), 1);
    Assert.assertEquals(loadMemberCount(new Path("/output/summary"),"20130331").longValue(),10L);
  }
  
  private void openDayForEvent(int year, int month, int day) throws IOException
  {
    System.out.println(String.format("start day: %04d %02d %02d",year,month,day));
    _eventWriter.open(year, month, day);
  }
  
  private void closeDayForEvent() throws IOException
  {
    _eventWriter.close();
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
    System.out.println("record: " + _record.toString());
    _eventWriter.append(_record);
  }
  
  private void checkIdCount(HashMap<Long,Integer> counts, long id, long count)
  {
    Assert.assertTrue(counts.containsKey(id));
    Assert.assertEquals(counts.get(id).intValue(), count);
  }
  
  private void checkSize(HashMap<Long,Integer> counts, int expectedSize)
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
  
  private void checkOutputFolderCount(Path path, int expectedCount) throws IOException
  {
    Assert.assertEquals(countOutputFolders(path),expectedCount,"Found: " + listOutputFolders(path));
  }
  
  private void checkIntermediateFolderCount(Path path, int expectedCount) throws IOException
  {
    Assert.assertEquals(countIntermediateFolders(path),expectedCount,"Found: " + listIntermediateFolders(path));
  }
  
  private int countIntermediateFolders(Path path) throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.globStatus(new Path(path,"*/*/*"),PathUtils.nonHiddenPathFilter).length;
  }
  
  private int countOutputFolders(Path path) throws IOException
  {
    FileSystem fs = getFileSystem();
    return fs.listStatus(path,PathUtils.nonHiddenPathFilter).length;
  }
  
  private String listOutputFolders(Path path) throws IOException
  {
    StringBuilder sb = new StringBuilder();
    for (FileStatus stat : getFileSystem().listStatus(path,PathUtils.nonHiddenPathFilter))
    {
      sb.append(stat.getPath().getName());
      sb.append(",");
    }
    return sb.toString();
  }
  
  private String listIntermediateFolders(Path path) throws IOException
  {
    StringBuilder sb = new StringBuilder();
    for (FileStatus stat : getFileSystem().globStatus(new Path(path,"*/*/*"),PathUtils.nonHiddenPathFilter))
    {
      sb.append(stat.getPath().getName());
      sb.append(",");
    }
    return sb.toString();
  }
  
  private Long loadMemberCount(Path path, String timestamp) throws IOException
  {
    FileSystem fs = getFileSystem();
    Assert.assertTrue(fs.exists(new Path(path, timestamp)));
    for (FileStatus stat : fs.globStatus(new Path(path,timestamp + "/*.avro")))
    {
      _log.info(String.format("found: %s (%d bytes)",stat.getPath(),stat.getLen()));
      FSDataInputStream is = fs.open(stat.getPath());
      DatumReader <GenericRecord> reader = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(is, reader);
      
      try
      {
        GenericRecord r = dataFileStream.next();
        Long count = (Long)((GenericRecord)r.get("value")).get("count");   
        Assert.assertNotNull(count);       
        System.out.println("found count: " + count);
        return count;
      }
      finally
      {
        dataFileStream.close();
      }
    }
    throw new RuntimeException("found no data");
  }
  
  private HashMap<Long,Integer> loadOutputCounts(Path path, String timestamp) throws IOException
  {
    HashMap<Long,Integer> counts = new HashMap<Long,Integer>();
    FileSystem fs = getFileSystem();
    Assert.assertTrue(fs.exists(new Path(path, timestamp)));
    for (FileStatus stat : fs.globStatus(new Path(path,timestamp + "/*.avro")))
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
          _log.info("found: " + r.toString());
          Long memberId = (Long)((GenericRecord)r.get("key")).get("member_id");
          Assert.assertNotNull(memberId);
          Integer count = (Integer)((GenericRecord)r.get("value")).get("count");   
          Assert.assertNotNull(count);     
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
