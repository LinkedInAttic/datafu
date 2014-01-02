package datafu.hourglass.test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import datafu.hourglass.avro.AvroDateRangeMetadata;
import datafu.hourglass.fs.DatePath;
import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;
import datafu.hourglass.jobs.PartitionCollapsingExecutionPlanner;
import datafu.hourglass.test.util.DailyTrackingWriter;

public class PartitionCollapsingExecutionPlannerTests extends TestBase
{
  private Logger _log = Logger.getLogger(PartitionCollapsingTests.class);
  
  private Path _inputPath = new Path("/input");
  private Path _outputPath = new Path("/output");
  
  private Properties _props;
  
  private static final Schema EVENT_SCHEMA;
  
  private DailyTrackingWriter _eventWriter;
  
  private int _maxDaysToProcess;
  private boolean _reusePreviousOutput;
  private String _startDate;
  private String _endDate;
  private Integer _numDays;
  private PartitionCollapsingExecutionPlanner _planner;
  
  static
  {    
    EVENT_SCHEMA = Schemas.createRecordSchema(PartitionCollapsingTests.class, "Event",
                                              new Field("id", Schema.create(Type.LONG), "ID", null));
  }
  
  public PartitionCollapsingExecutionPlannerTests() throws IOException
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
    
    _maxDaysToProcess = 365;
    _numDays = null;
    _startDate = null;
    _endDate = null;
    _reusePreviousOutput = false;

    _planner = null;
               
    _eventWriter = new DailyTrackingWriter(_inputPath,EVENT_SCHEMA,getFileSystem());
  }
  
  @Test
  public void exactlyThreeDays() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 3;
    
    createInput(2012,10,1);
    createInput(2012,10,2);
    createInput(2012,10,3);
    
    createPlan();
    
    checkInputSize(3);
    checkForInput(2012,10,1);
    checkForInput(2012,10,2);
    checkForInput(2012,10,3);    

    checkNewInputSize(3);
    checkForNewInput(2012,10,1);
    checkForNewInput(2012,10,2);
    checkForNewInput(2012,10,3);
    
    checkOldInputSize(0);
    checkReusingOutput(false);
  }
  
  /**
   * Tests that the most recent data is used. 
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void latestThreeDays() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 3;
    
    createInput(2012,10,1);
    createInput(2012,10,2);
    createInput(2012,10,3);
    createInput(2012,10,4);
    
    createPlan();
    
    checkInputSize(3);    
    checkForInput(2012,10,2);
    checkForInput(2012,10,3);
    checkForInput(2012,10,4);
    
    checkNewInputSize(3);
    checkForNewInput(2012,10,2);
    checkForNewInput(2012,10,3);
    checkForNewInput(2012,10,4);
    
    checkOldInputSize(0);
    checkReusingOutput(false);
  }
  
  /**
   * Tests that the previous output can be reused, even when there are two new days since the previous
   * result.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void previousOutputReuse() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 8;
    _reusePreviousOutput = true;
    
    createInput(2012,10,1);
    createInput(2012,10,2);
    createInput(2012,10,3);
    createInput(2012,10,4);
    createInput(2012,10,5);
    createInput(2012,10,6);
    createInput(2012,10,7);
    createInput(2012,10,8);
    createInput(2012,10,9);
    createInput(2012,10,10);
    
    createOutput(new DateRange(getDate(2012,10,1),getDate(2012,10,8)));
    
    createPlan();
    
    checkNewInputSize(2);
    checkForNewInput(2012,10,9);
    checkForNewInput(2012,10,10);
    
    checkOldInputSize(2);
    checkForOldInput(2012,10,1);
    checkForOldInput(2012,10,2);
    
    checkInputSize(4);    
    checkForInput(2012,10,1);
    checkForInput(2012,10,2);
    checkForInput(2012,10,9);
    checkForInput(2012,10,10);
    
    checkReusingOutput(true);
  }
  
  /**
   * Tests that the previous output will not be reused when the window size is small.
   * It is more work to reuse the previous output in this case because the old input has
   * to be subtracted from the previous result.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void previousOutputNoReuseSmallWindow() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 2;
    _reusePreviousOutput = true;
    
    createInput(2012,10,1);
    createInput(2012,10,2);
    createInput(2012,10,3);
    
    createOutput(new DateRange(getDate(2012,10,1),getDate(2012,10,2)));
    
    createPlan();
    
    checkNewInputSize(2);
    checkForNewInput(2012,10,2);
    checkForNewInput(2012,10,3);
    
    checkOldInputSize(0);
    
    checkInputSize(2);    
    checkForInput(2012,10,2);
    checkForInput(2012,10,3);
    
    checkReusingOutput(false);
  }
  
  /**
   * Tests that the previous output won't be reused when it is too old.  This would require subtracting off
   * all the old input data, then adding the new data.  It is better to just use the new data and not reuse
   * the previous output.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  @Test
  public void previousOutputNoReuseTooOld() throws IOException, InterruptedException, ClassNotFoundException
  {
    _numDays = 8;
    _reusePreviousOutput = true;
    
    for (int i=1; i<=20; i++)
    {
      createInput(2012,10,i);
    }
    
    // previous output too old to be useful
    createOutput(new DateRange(getDate(2012,10,1),getDate(2012,10,8)));
    
    createPlan();
    
    checkNewInputSize(8);
    for (int i=13; i<=20; i++)
    {
      checkForNewInput(2012,10,i);
    }
    
    checkOldInputSize(0);
    
    checkInputSize(8);    
    for (int i=13; i<=20; i++)
    {
      checkForInput(2012,10,i);
    }
    
    checkReusingOutput(false);
  }
  
  private void checkForInput(int year, int month, int day)
  {
    checkForPath(_planner.getInputsToProcess(),year,month,day);
  }
  
  private void checkForNewInput(int year, int month, int day)
  {
    checkForPath(_planner.getNewInputsToProcess(),year,month,day);
  }
  
  private void checkForOldInput(int year, int month, int day)
  {
    checkForPath(_planner.getOldInputsToProcess(),year,month,day);
  }
  
  private void checkForPath(List<DatePath> paths, int year, int month, int day)
  {
    Date date = getDate(year,month,day);
    DatePath datePath = DatePath.createNestedDatedPath(_inputPath.makeQualified(getFileSystem()),date);
    for (DatePath dp : paths)
    {
      if (dp.equals(datePath))
      {
        return;
      }
    }
    Assert.fail(String.format("Could not find %s",datePath));
  }
  
  private Date getDate(int year, int month, int day)
  {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month-1);
    cal.set(Calendar.DAY_OF_MONTH, day);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }
  
  private void checkInputSize(int size)
  {
    Assert.assertEquals(size,_planner.getInputsToProcess().size());
  }
  
  private void checkNewInputSize(int size)
  {
    Assert.assertEquals(size,_planner.getNewInputsToProcess().size());
  }
  
  private void checkOldInputSize(int size)
  {
    Assert.assertEquals(size,_planner.getOldInputsToProcess().size());
  }
  
  private void checkReusingOutput(boolean reuse)
  {
    Assert.assertEquals(reuse, _planner.getPreviousOutputToProcess() != null);
  }
  
  private void createInput(int year, int month, int day) throws IOException
  {
    _eventWriter.open(year, month, day);
    _eventWriter.close();
  }
  
  private void createOutput(DateRange dateRange) throws IOException
  {
    DataFileWriter<GenericRecord> dataWriter;
    OutputStream outputStream;
    
    Path path = new Path(_outputPath,PathUtils.datedPathFormat.format(dateRange.getEndDate()));
    
    Schema ouputSchema = Schemas.createRecordSchema(PartitionCollapsingTests.class, "Output",
                                              new Field("id", Schema.create(Type.LONG), "ID", null));
    
    outputStream = getFileSystem().create(new Path(path, "part-00000.avro"));
    
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
    dataWriter = new DataFileWriter<GenericRecord>(writer);      
    
    dataWriter.setMeta(AvroDateRangeMetadata.METADATA_DATE_START,
                       Long.toString(dateRange.getBeginDate().getTime()));
    
    dataWriter.setMeta(AvroDateRangeMetadata.METADATA_DATE_END,
                       Long.toString(dateRange.getEndDate().getTime()));
    
    dataWriter.create(ouputSchema, outputStream);
        
    // empty file
    
    dataWriter.close();
    outputStream.close();
    dataWriter = null;
    outputStream = null; 
  }
  
  private void createPlan() throws IOException, InterruptedException, ClassNotFoundException
  {
    _props = newTestProperties();
    
    _planner = new PartitionCollapsingExecutionPlanner(getFileSystem(),_props);
    
    _planner.setNumDays(_numDays);
    _planner.setMaxToProcess(_maxDaysToProcess);
    _planner.setInputPaths(Arrays.asList(_inputPath));
    _planner.setOutputPath(_outputPath);
    _planner.setReusePreviousOutput(_reusePreviousOutput);
    
    if (_startDate != null)
    {
      try
      {
        _planner.setStartDate(PathUtils.datedPathFormat.parse(_startDate));
      }
      catch (ParseException e)
      {
        Assert.fail(e.toString());
      }
    }
    
    if (_endDate != null)
    {
      try
      {
        _planner.setEndDate(PathUtils.datedPathFormat.parse(_endDate));
      }
      catch (ParseException e)
      {
        Assert.fail(e.toString());
      }
    }
    
    _planner.createPlan();
  }
}
