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

package datafu.pig.sampling;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * <p>
 * Performs a weighted random sample using an in-memory reservoir to produce
 * a weighted random sample of a given size based on the A-Res algorithm described in 
 * {@link <a href="http://utopia.duth.gr/~pefraimi/research/data/2007EncOfAlg.pdf" target="_blank">paper</a>}. 
 * </p>
 * <p>
 * Species with larger weight have higher probability to be selected in the final sample set.
 * </p>
 * <p>
 * This UDF inherits from {@link ReservoirSample} and it is guaranteed to produce
 * a sample of the given size.  Similarly it comes at the cost of scalability.
 * since it uses internal storage with size equaling the desired sample to guarantee the exact sample size.
 * </p>
 * <p>
 * Its constructor takes 2 arguments. 
 * <ul>
 *     <li>The 1st argument specifies the sample size which should be a string of positive integer.
 *     <li>The 2nd argument specifies the index of the weight field in the input tuple, 
 *     which should be a string of non-negative integer that is no greater than the input tuple size. 
 * </ul>
 * </p>
 * <p>
 * Example:
 * <pre>
 * {@code
 * define WeightedSample datafu.pig.sampling.WeightedReservoirSample('1','1');
 * input = LOAD 'input' AS (v1:chararray, v2:INT);
 * input_g = GROUP input ALL;
 * sampled = FOREACH input_g GENERATE WeightedSample(input);
 * }
 * </pre>
 * </p>
 * @author wjian
 */

@Nondeterministic
public class WeightedReservoirSample extends ReservoirSample {
    
    private Integer weightIdx;
    
    public WeightedReservoirSample(String strNumSamples, String strWeightIdx)
    {
        super(strNumSamples);
        this.weightIdx = Integer.parseInt(strWeightIdx);
        if(this.weightIdx < 0) {
            throw new IllegalArgumentException("Invalid negative index of weight field argument for WeightedReserviorSample constructor: " 
                                     + strWeightIdx);
        }
    }
    
    @Override
    protected ScoredTuple.ScoreGenerator getScoreGenerator()
    {
        if(super.scoreGen == null)
        {
            super.scoreGen = new InverseWeightScoreGenerator(this.weightIdx);
        }
        return this.scoreGen;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
      try {
        Schema.FieldSchema inputFieldSchema = input.getField(0);

        if (inputFieldSchema.type != DataType.BAG) {
          throw new RuntimeException("Expected a BAG as input");
        }
        
        Schema inputBagSchema = inputFieldSchema.schema;
        
        if (inputBagSchema.getField(0).type != DataType.TUPLE)
        {
            throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                   DataType.findTypeName(inputBagSchema.getField(0).type)));
        }
        
        Schema tupleSchema = inputBagSchema.getField(0).schema;
        
        if(tupleSchema == null) {
            throw new RuntimeException("The tuple of input bag has no schema");
        }
        
        List<Schema.FieldSchema> fieldSchemaList = tupleSchema.getFields();
        
        if(fieldSchemaList == null || fieldSchemaList.size() <= Math.max(0, this.weightIdx)) {
            throw new RuntimeException("The field schema of the input tuple is null " +
            		                   "or the tuple size is no more than the weight field index: "
                                       + this.weightIdx);
        }
        
        if(fieldSchemaList.get(this.weightIdx).type != DataType.INTEGER &&
           fieldSchemaList.get(this.weightIdx).type != DataType.LONG &&
           fieldSchemaList.get(this.weightIdx).type != DataType.FLOAT &&
           fieldSchemaList.get(this.weightIdx).type != DataType.DOUBLE)
        {
            String[] expectedTypes = new String[] {DataType.findTypeName(DataType.INTEGER),
                                                   DataType.findTypeName(DataType.LONG),
                                                   DataType.findTypeName(DataType.FLOAT),
                                                   DataType.findTypeName(DataType.DOUBLE)};
            throw new RuntimeException("Expect the type of the weight field of the input tuple to be of (" +
                    java.util.Arrays.toString(expectedTypes) + "), but instead found (" + 
                    DataType.findTypeName(fieldSchemaList.get(this.weightIdx).type) + "), weight field: " + 
                    this.weightIdx);
        } 
        
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                 inputFieldSchema.schema, DataType.BAG));    
      } catch (FrontendException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    
    String param = null;
    
    private String getParam()
    {
      if (this.param == null) {
          if(super.numSamples != null && this.weightIdx != null) {
              this.param = String.format("('%d','%d')", 
                                       super.numSamples,
                                       this.weightIdx);
          } else {
              this.param = "";
          }
      }
      
      return this.param;
    }

   
    @Override
    public String getInitial() 
    {
      return Initial.class.getName() + getParam();
    }
  
    @Override
    public String getIntermed() 
    {
      return Intermediate.class.getName() + getParam();
    }
    
    @Override
    public String getFinal() 
    {
      return Final.class.getName() + getParam();
    }
   
    static public class Initial extends ReservoirSample.Initial
    {
      private Integer weightIdx; 
        
      public Initial()
      {
          super();
          this.weightIdx = null;
      }
      
      public Initial(String strNumSamples, String strWeightIdx)
      {
          super(strNumSamples);
          this.weightIdx = Integer.parseInt(strWeightIdx);
          if(this.weightIdx < 0) {
              throw new IllegalArgumentException("Invalid negative index of weight field for WeightedReserviorSample.Initial constructor: " 
                                       + strWeightIdx);
          }
      }
      
      @Override
      protected ScoredTuple.ScoreGenerator getScoreGenerator()
      {
          if(super.scoreGen == null)
          {
              super.scoreGen = new InverseWeightScoreGenerator(this.weightIdx);
          }
          return super.scoreGen;
      }
    }
    
    static public class Intermediate extends ReservoirSample.Intermediate 
    {        
        public Intermediate()
        {
            super();
        }
        
        public Intermediate(String strNumSamples, String strWeightIdx)
        {
            super(strNumSamples);
        }        
    }
    
    static public class Final extends ReservoirSample.Final 
    {        
        public Final()
        {
            super();
        }
        
        public Final(String strNumSamples, String strWeightIdx)
        {
            super(strNumSamples);
        }        
    }

    static class InverseWeightScoreGenerator implements ScoredTuple.ScoreGenerator
    {        
        //index of the weight field of the input tuple
        private int weightIdx;
        
        InverseWeightScoreGenerator(Integer weightIdx) 
        {
            if(weightIdx == null || weightIdx < 0) {
                throw new IllegalArgumentException("Invalid null or negative weight index input: " + weightIdx);
            }
            this.weightIdx = weightIdx;
        }
        
        @Override
        public double generateScore(Tuple sample) throws ExecException
        {
            double weight = ((Number)sample.get(this.weightIdx)).doubleValue();
            if(Double.compare(weight, 0.0) <= 0)
            {
                //non-positive weight should be avoided
                throw new ExecException(String.format("Invalid sample weight [%f]. It should be a positive real number", weight));
            }
            //a differnt approach to try: u^(1/w) could be exp(log(u)/w) ?
            return Math.pow(Math.random(), 1/weight);
        }
    }
}
