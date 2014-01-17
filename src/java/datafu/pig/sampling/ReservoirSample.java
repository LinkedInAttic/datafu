/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *           http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package datafu.pig.sampling;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.base.Preconditions;

/**
 * Performs a simple random sample using an in-memory reservoir to produce
 * a uniformly random sample of a given size.
 * 
 * <p>
 * This is similar to {@link SimpleRandomSample}, however it is guaranteed to produce
 * a sample of the given size.  This comes at the cost of scalability.
 * {@link SimpleRandomSample} produces a sample of the desired size with likelihood of 99.99%,
 * while using less internal storage.  ReservoirSample on the other hand uses internal storage
 * with size equaling the desired sample to guarantee the exact sample size.
 * </p>
 * 
 * <p>
 * This algebraic implementation is backed by a heap and maintains the original roll in order
 * to compensate for skew.
 * </p>
 * 
 * @author wvaughan
 *
 */
@Nondeterministic
public class ReservoirSample extends AccumulatorEvalFunc<DataBag> implements Algebraic
{
  protected ScoredSampleReservoir reservoir;
  
  protected ReservoirSample(ScoredSampleReservoir reservoir)
  {
    Preconditions.checkArgument(reservoir != null, "Input reservoir instance should not be null");
    this.reservoir = reservoir;
  }
    
  public ReservoirSample(String numSamples)
  {
    this(new NoJumpScoredSampleReservoir(Integer.parseInt(numSamples)));    
  }
   
  @Override
  public void accumulate(Tuple input) throws IOException
  {
    DataBag samples = (DataBag) input.get(0);
    this.reservoir.consider(samples); 
  }

  @Override
  public void cleanup()
  {
    this.reservoir.clear();
  }

  @Override
  public DataBag getValue()
  {
    DataBag output = BagFactory.getInstance().newDefaultBag();  
    for (ScoredTuple sample : this.reservoir.getReservoir()) {
      output.add(sample.getTuple());
    }
    return output;
  }

  @Override
  public DataBag exec(Tuple input) throws IOException 
  {    
    DataBag samples = (DataBag)input.get(0);
    if (samples.size() <= this.reservoir.getNumSamples()) {
      return samples;
    }
    else
    {
      return super.exec(input);
    }
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG) {
        throw new RuntimeException("Expected a BAG as input");
      }
      
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                               inputFieldSchema.schema, DataType.BAG));    
    } catch (FrontendException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  String param = null;
  private String getParam() {
    if (param == null) {
      if (this.reservoir != null) {
        param = String.format("('%d')", this.reservoir.getNumSamples());
      } else {
        param = "";
      }
    }
    return param;
  }

  @Override
  public String getInitial() {
    return Initial.class.getName()+getParam();
  }

  @Override
  public String getIntermed() {
    return Intermediate.class.getName()+getParam();
  }

  @Override
  public String getFinal() {
    return Final.class.getName()+getParam();
  }
  
  static public class Initial extends EvalFunc<Tuple>
  {
    protected ScoredSampleReservoir reservoir;
    
    TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public Initial()
    {
        this.reservoir = null;
    }
    
    public Initial(String numSamples)
    {
      this.reservoir = new NoJumpScoredSampleReservoir(Integer.parseInt(numSamples));
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
      DataBag output = BagFactory.getInstance().newDefaultBag();
                  
      DataBag samples = (DataBag) input.get(0);

      if (samples == null)
      {
        // do nothing
      }
      else if (samples.size() <= this.reservoir.getNumSamples()) {
        // no need to construct a reservoir, so just emit intermediate tuples
        for (Tuple sample : samples) {
          // add the score on to the intermediate tuple
          output.add(new ScoredTuple(this.reservoir.getScoreGenerator().generateScore(sample), sample).getIntermediateTuple(tupleFactory));
        }
      } else {
        this.reservoir.consider(samples);
        
        for (ScoredTuple scoredTuple : this.reservoir.getReservoir()) {
          // add the score on to the intermediate tuple
          output.add(scoredTuple.getIntermediateTuple(tupleFactory));
        }
      }

      return tupleFactory.newTuple(output);
    }
    
  }
  
  static public class Intermediate extends EvalFunc<Tuple>
  {
    int numSamples;
    private Reservoir reservoir;
    TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public Intermediate(){}
    
    public Intermediate(String numSamples)
    {
      this.numSamples = Integer.parseInt(numSamples);
    }
    
    private Reservoir getReservoir()
    {
      if (reservoir == null) {
        reservoir = new Reservoir(this.numSamples);
      }
      return reservoir;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
      DataBag bagOfSamples = (DataBag) input.get(0);
      for (Tuple innerTuple : bagOfSamples) {
        DataBag samples = (DataBag) innerTuple.get(0);        
        
        for (Tuple sample : samples) {
          // use the same score as previously generated
          getReservoir().consider(ScoredTuple.fromIntermediateTuple(sample));
        }
      }
      
      DataBag output = BagFactory.getInstance().newDefaultBag();
      for (ScoredTuple scoredTuple : getReservoir()) {
        // add the score on to the intermediate tuple
        output.add(scoredTuple.getIntermediateTuple(tupleFactory));
      }

      return tupleFactory.newTuple(output);
    }
    
  }
  
  static public class Final extends EvalFunc<DataBag>
  {
    int numSamples;
    private Reservoir reservoir;
    TupleFactory tupleFactory = TupleFactory.getInstance();
    
    public Final(){}
    
    public Final(String numSamples)
    {
      this.numSamples = Integer.parseInt(numSamples);
    }
    
    private Reservoir getReservoir()
    {
      if (reservoir == null) {
        reservoir = new Reservoir(this.numSamples);
      }
      return reservoir;
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException {
      DataBag bagOfSamples = (DataBag) input.get(0);
      for (Tuple innerTuple : bagOfSamples) {
        DataBag samples = (DataBag) innerTuple.get(0);        
        
        for (Tuple sample : samples) {
          // use the same score as previously generated
          getReservoir().consider(ScoredTuple.fromIntermediateTuple(sample));
        }
      }
      
      DataBag output = BagFactory.getInstance().newDefaultBag();  
      for (ScoredTuple scoredTuple : getReservoir()) {
        // output the original tuple
        output.add(scoredTuple.getTuple());
      }

      return output;
    }    
  }
  
  static class NoJumpScoredSampleReservoir extends ScoredSampleReservoir
  {
      NoJumpScoredSampleReservoir(int numSamples) 
      {
          super(numSamples);
      }
      
      ScoreGenerator getScoreGenerator() 
      {
          if(super.scoreGen == null) {
              super.scoreGen = new PureRandomScoreGenerator();
          }
          return super.scoreGen;
      }
      
      void consider(DataBag samples) throws ExecException 
      {
          if(samples != null) {
              ScoreGenerator scoreGen = getScoreGenerator();
              for (Tuple sample : samples) {
                  getReservoir().consider(new ScoredTuple(scoreGen.generateScore(sample), sample));
              }
          }
      }
  }
}
