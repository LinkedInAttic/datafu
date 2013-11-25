/*
 * Copyright 2010 LinkedIn Corp. and contributors
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
 
package datafu.pig.linkanalysis;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A UDF which implements {@link <a href="http://en.wikipedia.org/wiki/PageRank" target="_blank">PageRank</a>}.
 * 
 * <p>  
 * This is not a distributed implementation.  Each graph is stored in memory while running the algorithm, with edges optionally 
 * spilled to disk to conserve memory.  This can be used to distribute the execution of PageRank on multiple
 * reasonably sized graphs.  It does not distribute execuion of PageRank for each individual graph.  Each graph is identified
 * by an integer valued topic ID.
 * </p>
 * 
 * <p>
 * If the graph is too large to fit in memory than an alternative method must be used, such as an iterative approach which runs
 * many MapReduce jobs in a sequence to complete the PageRank iterations.
 * </p>
 * 
 * <p>
 * Each graph is represented through a bag of (source,edges) tuples.  The 'source' is an integer ID representing the source node.
 * The 'edges' are the outgoing edges from the source node, represented as a bag of (dest,weight) tuples.  The 'dest' is an
 * integer ID representing the destination node.  The weight is a double representing how much the edge should be weighted.
 * For a standard PageRank implementation just use weight of 1.0.
 * </p>
 * 
 * <p>
 * The output of the UDF is a bag of (source,rank) pairs, where 'rank' is the PageRank value for that source in the graph.
 * </p>
 * 
 * <p>
 * There are several configurable options for this UDF, among them:
 * <p>
 * 
 * <ul>
 * <li>
 * <b>alpha</b>: Controls the PageRank alpha value.  The default is 0.85.  A higher value reduces the "random jump"
 * factor and causes the rank to be influenced more by edges. 
 * </li>
 * <li>
 * <b>max_iters</b>: The maximum number of iterations to run.  The default is 150.
 * </li>
 * <li>
 * <b>dangling_nodes</b>: How to handling "dangling nodes", i.e. nodes with no outgoing edges.  When "true" this is equivalent
 * to forcing a dangling node to have an outgoing edge to every other node in the graph.  The default is "false".
 * </li>
 * <li>
 * <b>tolerance</b>: A threshold which causes iterations to cease.  It is measured from the total change in ranks from each of
 * the nodes in the graph.  As the ranks settle on their final values the total change decreases.  This can be used
 * to stop iterations early.  The default is 1e-16. 
 * </li> 
 * <li>
 * <b>max_nodes_and_edges</b>: This is a control to prevent running out of memory.  As a graph is loaded, if the sum of edges
 * and nodes exceeds this value then it will stop.  It will not fail but PageRank will not be run on this graph.  Instead a null
 * value will be returned as a result.  The default is 100M.
 * </li>
 * <li>
 * <b>spill_to_edge_disk_storage</b>: Used to conserve memory.  When "true" it causes the edge data to be written to disk in a temp file instead
 * of being held in memory when the number of edges exceeds a threshold.  The nodes are still held in memory however.  
 * Each iteration of PageRank will stream through the edges stored on disk.  The default is "false".
 * </li>
 * <li>
 * <b>max_edges_in_memory</b>: When spilling edges to disk is enabled, this is the threshold which triggers that behavior.  The default is 30M.
 * </li>
 * </ul>
 * 
 * <p>
 * Parameters are configured by passing them in as a sequence of pairs into the UDF constructor.  For example, below the alpha value is set to
 * 0.87 and dangling nodes are enabled.  All arguments must be strings.
 * </p>
 * 
 * <p>
 * <pre>
 * {@code
 * define PageRank datafu.pig.linkanalysis.PageRank('alpha','0.87','dangling_nodes','true');
 * }
 * </pre>
 * </p>
 * 
 * <p>
 * Full example:
 * <pre>
 * {@code
 * 
 * topic_edges = LOAD 'input_edges' as (topic:INT,source:INT,dest:INT,weight:DOUBLE);
 * 
 * topic_edges_grouped = GROUP topic_edges by (topic, source) ;
 * topic_edges_grouped = FOREACH topic_edges_grouped GENERATE
 *    group.topic as topic,
 *    group.source as source,
 *    topic_edges.(dest,weight) as edges;
 * 
 * topic_edges_grouped_by_topic = GROUP topic_edges_grouped BY topic; 
 * 
 * topic_ranks = FOREACH topic_edges_grouped_by_topic GENERATE
 *    group as topic,
 *    FLATTEN(PageRank(topic_edges_grouped.(source,edges))) as (source,rank);
 *
 * topic_ranks = FOREACH topic_ranks GENERATE
 *    topic, source, rank;
 * 
 * }
 * </pre>
 * </p> 
 */
public class PageRank extends AccumulatorEvalFunc<DataBag>
{
  private final datafu.pig.linkanalysis.PageRankImpl graph = new datafu.pig.linkanalysis.PageRankImpl();

  private int maxNodesAndEdges = 100000000;
  private int maxEdgesInMemory = 30000000;
  private double tolerance = 1e-16;
  private int maxIters = 150;
  private boolean useEdgeDiskStorage = false;
  private boolean enableDanglingNodeHandling = false;
  private boolean enableNodeBiasing = false;
  private boolean aborted = false;
  private float alpha = 0.85f;

  TupleFactory tupleFactory = TupleFactory.getInstance();
  BagFactory bagFactory = BagFactory.getInstance();
  
  public PageRank()
  {
    initialize();
  }

  public PageRank(String... parameters)
  {
    if (parameters.length % 2 != 0)
    {
      throw new RuntimeException("Invalid parameters list");
    }

    for (int i=0; i<parameters.length; i+=2)
    {
      String parameterName = parameters[i];
      String value = parameters[i+1];
      if (parameterName.equals("max_nodes_and_edges"))
      {
        maxNodesAndEdges = Integer.parseInt(value);
      }
      else if (parameterName.equals("max_edges_in_memory"))
      {
        maxEdgesInMemory = Integer.parseInt(value);
      }
      else if (parameterName.equals("tolerance"))
      {
        tolerance = Double.parseDouble(value);
      }
      else if (parameterName.equals("max_iters"))
      {
        maxIters = Integer.parseInt(value);
      }
      else if (parameterName.equals("spill_to_edge_disk_storage"))
      {
        useEdgeDiskStorage = Boolean.parseBoolean(value);
      }
      else if (parameterName.equals("dangling_nodes"))
      {
        enableDanglingNodeHandling = Boolean.parseBoolean(value);
      }
      else if (parameterName.equals("node_biasing"))
      {
        enableNodeBiasing = Boolean.parseBoolean(value);
      }
      else if (parameterName.equals("alpha"))
      {
        alpha = Float.parseFloat(value);
      }
    }

    initialize();
  }

  private void initialize()
  {
    if (useEdgeDiskStorage)
    {
      this.graph.enableEdgeDiskCaching();
    }
    else
    {
      this.graph.disableEdgeDiskCaching();
    }

    if (enableDanglingNodeHandling)
    {
      this.graph.enableDanglingNodeHandling();
    }
    else
    {
      this.graph.disableDanglingNodeHandling();
    }
    
    if (enableNodeBiasing)
    {
      this.graph.enableNodeBiasing();
    }
    else
    {
      this.graph.disableNodeBiasing();
    }

    this.graph.setEdgeCachingThreshold(maxEdgesInMemory);
    this.graph.setAlpha(alpha);
  }

  @Override
  public void accumulate(Tuple t) throws IOException
  {
    if (aborted)
    {
      return;
    }
    
    DataBag bag = (DataBag) t.get(0);
    if (bag == null || bag.size() == 0)
      return;
    
    for (Tuple sourceTuple : bag) 
    {
      Integer sourceId = (Integer)sourceTuple.get(0);
      DataBag edges = (DataBag)sourceTuple.get(1);
      Double nodeBias = null;
      if (enableNodeBiasing)
      {
        nodeBias = (Double)sourceTuple.get(2);
      }

      ArrayList<Map<String,Object>> edgesMapList = new ArrayList<Map<String, Object>>();

      for (Tuple edgeTuple : edges)
      {
        Integer destId = (Integer)edgeTuple.get(0);
        Double weight = (Double)edgeTuple.get(1);
        HashMap<String,Object> edgeMap = new HashMap<String, Object>();
        edgeMap.put("dest",destId);
        edgeMap.put("weight",weight);
        edgesMapList.add(edgeMap);
      }

      if (enableNodeBiasing)
      {
        graph.addNode(sourceId, edgesMapList, nodeBias.floatValue());
      }
      else
      {
        graph.addNode(sourceId, edgesMapList);
      }

      if (graph.nodeCount() + graph.edgeCount() > maxNodesAndEdges)
      {
        System.out.println(String.format("There are too many nodes and edges (%d + %d > %d). Aborting.", graph.nodeCount(), graph.edgeCount(), maxNodesAndEdges));
        aborted = true;
        break;
      }

      reporter.progress();
    }
  }

  @Override
  public DataBag getValue()
  {
    if (aborted)
    {
      return null;
    }
    
    System.out.println(String.format("Nodes: %d, Edges: %d", graph.nodeCount(), graph.edgeCount()));
    
    ProgressIndicator progressIndicator = getProgressIndicator();
    System.out.println("Finished loading graph.");
    long startTime = System.nanoTime();
    System.out.println("Initializing.");
    try
    {
      graph.init(progressIndicator);
    }
    catch (IOException e)
    {
      e.printStackTrace();
      return null;
    }
    System.out.println(String.format("Done, took %f ms", (System.nanoTime() - startTime)/10.0e6));

    float totalDiff;
    int iter = 0;

    System.out.println("Beginning iterations");
    startTime = System.nanoTime();
    do
    {
      // TODO log percentage complete every 5 minutes
      try
      {
        totalDiff = graph.nextIteration(progressIndicator);
      }
      catch (IOException e)
      {
        e.printStackTrace();
        return null;
      }
      iter++;
    } while(iter < maxIters && totalDiff > tolerance);
    System.out.println(String.format("Done, %d iterations took %f ms", iter, (System.nanoTime() - startTime)/10.0e6));

    DataBag output = bagFactory.newDefaultBag();

    for (Int2IntMap.Entry node : graph.getNodeIds())
    {
      int nodeId = node.getIntKey();
      float rank = graph.getNodeRank(nodeId);
      List nodeData = new ArrayList(2);
      nodeData.add(nodeId);
      nodeData.add(rank);
      output.add(tupleFactory.newTuple(nodeData));
    }

    return output;
  }

  @Override
  public void cleanup()
  {
    try
    {
      aborted = false;
      this.graph.clear();
    }
    catch (IOException e)
    { 
      e.printStackTrace();
    }
  }

  private ProgressIndicator getProgressIndicator()
  {
    return new ProgressIndicator()
        {
          @Override
          public void progress()
          {
            reporter.progress();
          }
        };
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try
    {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }

      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      if (!this.enableNodeBiasing)
      {
        if (inputTupleSchema.size() != 2)
        {
          throw new RuntimeException("Expected two fields for the node data");
        }
      }
      else
      {
        if (inputTupleSchema.size() != 3)
        {
          throw new RuntimeException("Expected three fields for the node data");
        }
      }
      
      if (inputTupleSchema.getField(0).type != DataType.INTEGER)
      {
        throw new RuntimeException(String.format("Expected source to be an INTEGER, but instead found %s",
                                                 DataType.findTypeName(inputTupleSchema.getField(0).type)));
      }

      if (inputTupleSchema.getField(1).type != DataType.BAG)
      {
        throw new RuntimeException(String.format("Expected edges to be represented with a BAG"));
      }
      
      if (this.enableNodeBiasing && inputTupleSchema.getField(2).type != DataType.DOUBLE)
      {
        throw new RuntimeException(String.format("Expected node bias to be a DOUBLE, but instead found %s",
                                                 DataType.findTypeName(inputTupleSchema.getField(2).type)));
      }

      Schema.FieldSchema edgesFieldSchema = inputTupleSchema.getField(1);

      if (edgesFieldSchema.schema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected edges field to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(edgesFieldSchema.schema.getField(0).type)));
      }
      
      Schema edgesTupleSchema = edgesFieldSchema.schema.getField(0).schema;
      
      if (edgesTupleSchema.size() != 2)
      {
        throw new RuntimeException("Expected two fields for the edge data");
      }
      
      if (edgesTupleSchema.getField(0).type != DataType.INTEGER)
      {
        throw new RuntimeException(String.format("Expected destination edge ID to an INTEGER, but instead found %s",
                                                 DataType.findTypeName(edgesTupleSchema.getField(0).type)));
      }

      if (edgesTupleSchema.getField(1).type != DataType.DOUBLE)
      {
        throw new RuntimeException(String.format("Expected destination edge weight to a DOUBLE, but instead found %s",
                                                 DataType.findTypeName(edgesTupleSchema.getField(1).type)));
      }

      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("node",DataType.INTEGER));
      tupleSchema.add(new Schema.FieldSchema("rank",DataType.FLOAT));

      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                                 .getName()
                                                                 .toLowerCase(), input),
                                               tupleSchema,
                                               DataType.BAG));
    }
    catch (FrontendException e)
    {
      throw new RuntimeException(e);
    }
  }
}
