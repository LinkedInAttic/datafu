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

package datafu.hourglass.avro;

import java.io.IOException;
import java.util.Date;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datafu.hourglass.fs.DateRange;
import datafu.hourglass.fs.PathUtils;

/**
 * Manages the storage and retrieval of date ranges in the metadata of Avro files.
 * This is used by {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob} so that when reusing previous
 * output it can determine the date range the data corresponds to.
 * 
 * @author "Matthew Hayes"
 *
 */
public class AvroDateRangeMetadata
{
  public static String METADATA_DATE_START = "hourglass.date.start";
  public static String METADATA_DATE_END = "hourglass.date.end";
  
  /**
   * Reads the date range from the metadata stored in an Avro file.
   * 
   * @param fs file system to access path
   * @param path path to get date range for
   * @return date range
   * @throws IOException
   */
  public static DateRange getOutputFileDateRange(FileSystem fs, Path path) throws IOException
  {
    path = fs.listStatus(path, PathUtils.nonHiddenPathFilter)[0].getPath();
    FSDataInputStream dataInputStream = fs.open(path);
    DatumReader <GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(dataInputStream, reader);
    
    try
    {
      return new DateRange(new Date(Long.parseLong(dataFileStream.getMetaString(METADATA_DATE_START))),
                           new Date(Long.parseLong(dataFileStream.getMetaString(METADATA_DATE_END))));
    }
    finally
    {
      dataFileStream.close();
      dataInputStream.close();
    }
  }
  
  /**
   * Updates the Hadoop configuration so that the Avro files which are written have date range
   * information stored in the metadata.  This should be used in conjunction with 
   * {@link AvroKeyValueWithMetadataRecordWriter}.
   * 
   * @param conf configuration to store date range in
   * @param dateRange date range
   */
  public static void configureOutputDateRange(Configuration conf, DateRange dateRange)
  {
    // store the date range in the output file's metadata
    conf.set(AvroKeyValueWithMetadataRecordWriter.TEXT_PREFIX + METADATA_DATE_START, Long.toString(dateRange.getBeginDate().getTime()));
    conf.set(AvroKeyValueWithMetadataRecordWriter.TEXT_PREFIX + METADATA_DATE_END, Long.toString(dateRange.getEndDate().getTime()));
  }
}
