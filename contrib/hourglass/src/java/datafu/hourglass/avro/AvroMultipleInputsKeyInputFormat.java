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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A MapReduce InputFormat that can handle Avro container files and multiple inputs.
 * The input schema is determine based on the split.  The mapping from input path
 * to schema is stored in the job configuration.
 *
 * <p>Keys are AvroKey wrapper objects that contain the Avro data.  Since Avro
 * container files store only records (not key/value pairs), the value from
 * this InputFormat is a NullWritable.</p>
 */
public class AvroMultipleInputsKeyInputFormat<T> extends FileInputFormat<AvroKey<T>, NullWritable> 
{  
  /** {@inheritDoc} */
  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException 
  {    
    Schema readerSchema = AvroMultipleInputsUtil.getInputKeySchemaForSplit(context.getConfiguration(), split);
    if (readerSchema == null)
    {
      throw new RuntimeException("Could not determine input schema");
    }
    return new AvroKeyRecordReader<T>(readerSchema);
  }
}
