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
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datafu.hourglass.test.Schemas;

/**
 * Generate random test data in yyyy/MM/dd paths for
 * a given date range.
 * 
 * @author "Matthew Hayes"
 *
 */
public class GenerateIds extends Configured implements NamedTool
{
  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

  private static final Schema EVENT_SCHEMA;
  
  private Random random = new Random();
  
  static
  {    
    EVENT_SCHEMA = Schemas.createRecordSchema(GenerateIds.class, "Event",
                                              new Field("id", Schema.create(Type.LONG), null, null));
  }
  
  @Override
  public int run(String[] args) throws Exception
  {    
    if (args.length != 2)
    {
      System.err.printf("%s   %s\n",getName(),getDescription());
      System.err.println("Usage: <output-path> <date-range>");
      return 1;
    }
    
    try
    {
      return run(super.getConf(), args[0], args[1]);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
    }
    return 1;
  }
  
  public int run(Configuration conf, String outputPathString, String dateRange) throws IOException, InterruptedException, ClassNotFoundException
  {
    FileSystem fs = FileSystem.get(conf);
    Path outputPath = new Path(outputPathString);
    
    String[] dateRangeParts = dateRange.split("-");
    
    Date startDate;
    Date endDate = null;
    
    if (dateRangeParts.length == 1)
    {
      try
      {
        startDate = dateFormat.parse(dateRangeParts[0]);
      }
      catch (ParseException e)
      {
        System.err.println("Invalid date range: " + dateRangeParts[0]);
        return 1;
      }
    }
    else if (dateRangeParts.length ==2)
    {
      try
      {
        startDate = dateFormat.parse(dateRangeParts[0]);
      }
      catch (ParseException e)
      {
        System.err.println("Invalid date range: " + dateRangeParts[0]);
        return 1;
      }
      
      try
      {
        endDate = dateFormat.parse(dateRangeParts[1]);
      }
      catch (ParseException e)
      {
        System.err.println("Invalid date range: " + dateRangeParts[1]);
        return 1;
      }
      
      if (startDate.compareTo(endDate) >= 0)
      {
        System.err.println("Start date must be before end date");
        return 1;
      }
    }
    else
    {
      System.err.println("Invalid date range: " + dateRange);
      return 1;
    }
    
    Calendar cal = Calendar.getInstance();
    
    if (endDate == null)
    {
      createDataForDate(fs,outputPath,startDate);
    }
    else
    {
      for (Date date=startDate; date.compareTo(endDate) <= 0; )
      {
        createDataForDate(fs, outputPath, date);        
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, 1);
        date = cal.getTime();
      }
    }
        
    return 0;
  }
  
  private void createDataForDate(FileSystem fs, Path outputPath, Date date) throws IOException
  {
    // make sure output path exists
    if (!fs.exists(outputPath))
    {
      fs.mkdirs(outputPath);
    }
    
    Path datePath = new Path(outputPath,dateFormat.format(date));
    
    System.out.println("Writing to " + datePath.toString());
    
    DataFileWriter<GenericRecord> dataWriter;
    OutputStream outputStream;
    
    Path dailyPath = outputPath;
    Path path = new Path(dailyPath,dateFormat.format(date));
    
    // delete directory if it already exists
    if (fs.exists(path))
    {
      fs.delete(path, true);
    }
    
    outputStream = fs.create(new Path(path, "part-00000.avro"));
    
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
    dataWriter = new DataFileWriter<GenericRecord>(writer);        
    dataWriter.create(EVENT_SCHEMA, outputStream);
    
    GenericRecord record = new GenericData.Record(EVENT_SCHEMA);
    // create 1000 random IDs
    for (int i=0; i<1000; i++)
    {
      record.put("id", (long)(1 + random.nextInt(100)));
      dataWriter.append(record);
    }
    
    dataWriter.close();
    outputStream.close();
  }
  
  @Override
  public String getName()
  {
    return "generate";
  }

  @Override
  public String getDescription()
  {
    return "create random event data";
  }
}
