/*
 * Copyright 2013 LinkedIn Corp. and contributors
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

package datafu.test.pig.linkanalysis;


import static org.testng.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;


import datafu.test.pig.linkanalysis.PageRankImplTests;
import datafu.test.pig.PigTests;

public class PageRankTests extends PigTests
{
  /**
  register $JAR_PATH

  -- Need to enable dangling node handling since the Wikipedia example has them,
  -- otherwise the ranks won't be right. 
  define PageRank datafu.pig.linkanalysis.PageRank('dangling_nodes','true');
  
  data = LOAD 'input' AS (topic:INT,source:INT,dest:INT,weight:DOUBLE);
  
  data_grouped = GROUP data by (topic,source);
  
  data_grouped = foreach data_grouped {
    generate group.topic as topic, group.source as source, data.(dest,weight) as edges;
  };
  
  data_grouped2 = GROUP data_grouped by topic;
  data_grouped2 = foreach data_grouped2 {
    generate group as topic, FLATTEN(PageRank(data_grouped.(source,edges))) as (source,rnk);
  };
  
  data_grouped3 = FOREACH data_grouped2 GENERATE
    topic,
    source,
    rnk;
    
  STORE data_grouped3 INTO 'output';


   */
  @Multiline private String pageRankTest;
  
  @Test
  public void pigPageRankTest() throws Exception
  {
    PigTest test = createPigTestFromString(pageRankTest);

    String[] edges = PageRankImplTests.getWikiExampleEdges();

    Map<String,Integer> nodeIds = new HashMap<String,Integer>();
    Map<Integer,String> nodeIdsReversed = new HashMap<Integer,String>();
    Map<String,Float> expectedRanks = PageRankImplTests.parseExpectedRanks(PageRankImplTests.getWikiExampleExpectedRanks());

    File f = new File(System.getProperty("user.dir"), "input").getAbsoluteFile();
    if (f.exists())
    {
      f.delete();
    }

    FileWriter writer = new FileWriter(f);
    BufferedWriter bufferedWriter = new BufferedWriter(writer);

    for (String edge : edges)
    {
      String[] edgeParts = edge.split(" ");
      String source = edgeParts[0];
      String dest = edgeParts[1];
      if (!nodeIds.containsKey(source))
      {
        int id = nodeIds.size();
        nodeIds.put(source,id);
        nodeIdsReversed.put(id, source);
      }
      if (!nodeIds.containsKey(dest))
      {
        int id = nodeIds.size();
        nodeIds.put(dest,id);
        nodeIdsReversed.put(id, dest);
      }
      Integer sourceId = nodeIds.get(source);
      Integer destId = nodeIds.get(dest);

      StringBuffer sb = new StringBuffer();

      sb.append("1\t"); // topic
      sb.append(sourceId.toString() + "\t");
      sb.append(destId.toString() + "\t");
      sb.append("1.0\n"); // weight

      bufferedWriter.write(sb.toString());
    }

    bufferedWriter.close();

    test.runScript();
    Iterator<Tuple> tuples = test.getAlias("data_grouped3");

    System.out.println("Final node ranks:");
    int nodeCount = 0;
    while (tuples.hasNext())
    {
      Tuple nodeTuple = tuples.next();

      Integer topic = (Integer)nodeTuple.get(0);
      Integer nodeId = (Integer)nodeTuple.get(1);
      Float nodeRank = (Float)nodeTuple.get(2);

      assertEquals(1, topic.intValue());

      System.out.println(String.format("%d => %f", nodeId, nodeRank));

      Float expectedNodeRank = expectedRanks.get(nodeIdsReversed.get(nodeId));

      assertTrue(Math.abs(expectedNodeRank - nodeRank * 100.0f) < 0.1,
                 String.format("expected: %f, actual: %f", expectedNodeRank, nodeRank));

      nodeCount++;
    }

    assertEquals(nodeIds.size(),nodeCount);
  }
}
