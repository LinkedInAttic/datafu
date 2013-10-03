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

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main
{
  private final Map<String,NamedTool> tools;
  private int maxLength;
  
  public Main()
  {
    this.tools = new TreeMap<String,NamedTool>();
    
    maxLength = 0;
    for (NamedTool tool : new NamedTool[] {
      new CountById(),
      new EstimateCardinality(),
      new GenerateIds()
    }) {
      this.tools.put(tool.getName(), tool);
      maxLength = Math.max(maxLength, tool.getName().length());
    }
  }
  
  public static void main(String[] args)
  {
    int result = new Main().run(args);
    System.exit(result);
  }
  
  private int run(String[] args)
  {
    if (args.length > 0)
    {
      String name = args[0];
      NamedTool tool = this.tools.get(name);
      if (tool != null)
      {
        try
        {
          Configuration conf = new Configuration();
          return ToolRunner.run(conf, tool, Arrays.asList(args).subList(1, args.length).toArray(new String[]{}));
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
      else
      {
        System.err.printf("No tool exists by name '%s'\n",name);
      }
    }
    
    System.err.println("Hourglass demo tool\n");

    System.err.println("Available options:\n");
    for (NamedTool tool : this.tools.values())
    {
      System.err.printf("  %" + maxLength + "s      %s\n", tool.getName(), tool.getDescription());
    }
    
    System.err.println();
    
    return 1;
  }
}
