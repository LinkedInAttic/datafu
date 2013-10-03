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

package datafu.hourglass.test.jobs;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


import datafu.hourglass.test.Schemas;
import datafu.hourglass.test.jobs.counting.CountWriter;
import datafu.hourglass.test.jobs.counting.PartitionCollapsingIncrementalCountJob;

public class SimplePartitionCollapsingCountJob2 extends PartitionCollapsingIncrementalCountJob
{
  private static final Schema KEY_SCHEMA;
  
  static
  {
    KEY_SCHEMA = Schemas.createKeyRecordSchema(SimplePartitionCollapsingCountJob2.class,
                                       new Field("id", Schema.create(Type.LONG), "ID", null));
  }
  
  @Override
  protected Schema getKeySchema()
  {
    return KEY_SCHEMA;
  }
  
  public SimplePartitionCollapsingCountJob2(String name, Properties props) throws IOException
  {
    super(name, props);
  }
  
  private static AbstractCounter createCounter()
  {
    return new AbstractCounter()
    {    
      @Override
      protected void count(GenericRecord record, CountWriter writer) throws IOException, InterruptedException
      {
        GenericRecord key = new GenericData.Record(KEY_SCHEMA);
        key.put("id", record.get("id"));
        writer.count(key);
      }
    };
  }
  
  @Override
  protected AbstractCounter getCounter()
  {
    return createCounter();
  }
}

