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

package datafu.hourglass.test.util;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TimestampDataWriter
{
  private final Path _outputPath;
  private final Schema _schema;
  private final FileSystem _fs;
    
  private DataFileWriter<GenericRecord> _dataWriter;
  private OutputStream _outputStream;
  
  public TimestampDataWriter(Path outputPath, Schema schema, FileSystem fs)
  {
    _outputPath = outputPath;
    _schema = schema;
    _fs = fs;
  }
  
  public void open(int year, int month, int day) throws IOException
  {
    if (_dataWriter != null)
    {
      throw new RuntimeException("Already have data writer");
    }
  
    Path path = new Path(_outputPath,String.format("%04d%02d%02d",year,month,day));
    
    _outputStream = _fs.create(new Path(path, "part-00000.avro"));
    
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
    _dataWriter = new DataFileWriter<GenericRecord>(writer);        
    _dataWriter.create(_schema, _outputStream);
  }
  
  public void append(GenericRecord record) throws IOException
  {
    if (_dataWriter == null)
    {
      throw new RuntimeException("No data writer");
    }
    _dataWriter.append(record);
  }
  
  public void close() throws IOException
  {
    if (_dataWriter == null)
    {
      throw new RuntimeException("No data writer");
    }
    _dataWriter.close();
    _outputStream.close();
    _dataWriter = null;
    _outputStream = null; 
  }
}
