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

package datafu.pig.stats.entropy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.stats.entropy.stream.EntropyEstimator;
import datafu.pig.stats.entropy.EntropyUtil;


/**
 * Calculate the empirical entropy given a bag of data sample counts following entropy's
 * {@link <a href="http://en.wikipedia.org/wiki/Entropy_%28information_theory%29" target="_blank">wiki definition</a>} 
 * 
 * <p>
 * It supports entropy calculation both in a streaming way and in a distributed way using combiner.
 * </p>
 * 
 * <p>
 * This UDF's constructor accepts the logarithm base as its single argument. 
 * The definition of supported logarithm base is the same as {@link datafu.pig.stats.entropy.stream.StreamingEntropy}
 * </p>
 * 
 * <p>
 * Note: 
 * <ul>
 *     <li>the input to the UDF is a bag of data sample's occurrence frequency, 
 *     which is different from * {StreamingEntropy}, 
 *     whose input is a sorted bag of raw data samples.
 *     <li>each tuple in the UDF's input bag could be int, long, float, double, chararray, bytearray.
 *     The UDF will try to convert the input tuple's value to long type number.
 *     <li>if the UDF fails to convert the input tuple's value to long type number, the record will be
 *     skipped and a warning message will be written to the job's log.
 *     <li>the returned entropy value is of double type.
 * </ul>
 * </p>
 * 
 * <p>
 * How to use: 
 * </p>
 * <p>
 * This UDF is suitable to calculate entropy on the whole data set when we 
 * could easily get the each sample's frequency using an outer GROUP BY. 
 * </p>
 * <p>
 * Then we could use another outer GROUP BY on the sample frequencies to get the entropy. 
 * </p>
 * <p>
 * Example:
 * <pre>
 * 
 * {@code
 * 
 * define Entropy datafu.pig.stats.entropy.Entropy();
 *
 * input = LOAD 'input' AS (val: double);
 *
 * -- calculate the occurrence of each sample
 * counts_g = GROUP input BY val;
 * counts = FOREACh counts_g GENERATE COUNT(input) AS cnt;
 * 
 * -- calculate entropy 
 * input_counts_g = GROUP counts ALL;
 * entropy = FOREACH input_counts_g GENERATE Entropy(counts) AS entropy;
 * }
 * </pre>
 * </p>
 * <p>
 * Use case to calculate mutual information using Entropy:
 * <pre>
 * {@code
 * 
 * define Entropy datafu.pig.stats.entropy.Entropy();
 * 
 * input = LOAD 'input' AS (valX: double, valY: double);
 * 
 * ------------
 * -- calculate mutual information I(X, Y) using entropy
 * -- I(X, Y) = H(X) + H(Y) -  H(X, Y)
 * ------------
 * 
 * input_x_y_g = GROUP input BY (valX, valY);
 * input_x_y_cnt = FOREACH input_x_y_g GENERATE flatten(group) as (valX, valY), COUNT(input) AS cnt;
 * 
 * input_x_g = GROUP input_x_y_cnt BY valX;
 * input_x_cnt = FOREACH input_x_g GENERATE flatten(group) as valX, SUM(input_x_y_cnt.cnt) AS cnt;
 * 
 * input_y_g = GROUP input_x_y_cnt BY valY;
 * input_y_cnt = FOREACH input_y_g GENERATE flatten(group) as valY, SUM(input_x_y_cnt.cnt) AS cnt;
 * 
 * input_x_y_entropy_g = GROUP input_x_y_cnt ALL;
 * input_x_y_entropy = FOREACH input_x_y_entropy_g {
 *                         input_x_y_entropy_cnt = input_x_y_cnt.cnt;
 *                         GENERATE Entropy(input_x_y_entropy_cnt) AS x_y_entropy;
 *                     }
 *                         
 * input_x_entropy_g = GROUP input_x_cnt ALL;
 * input_x_entropy = FOREACH input_x_entropy_g {
 *                         input_x_entropy_cnt = input_x_cnt.cnt;
 *                         GENERATE Entropy(input_x_entropy_cnt) AS x_entropy;
 *                   }
 *                       
 * input_y_entropy_g = GROUP input_y_cnt ALL;
 * input_y_entropy = FOREACH input_y_entropy_g {
 *                         input_y_entropy_cnt = input_y_cnt.cnt;
 *                         GENERATE Entropy(input_y_entropy_cnt) AS y_entropy;
 *                   }
 *
 * input_mi_cross = CROSS input_x_y_entropy, input_x_entropy, input_y_entropy;
 * input_mi = FOREACH input_mi_cross GENERATE (input_x_entropy::x_entropy +
 *                                             input_y_entropy::y_entropy - 
 *                                             input_x_y_entropy::x_y_entropy) AS mi;
 * }
 * </pre>
 * </p>
 * 
 * @see datafu.pig.stats.entropy.stream.StreamingEntropy
 */

public class Entropy extends AccumulatorEvalFunc<Double> implements Algebraic {
    
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    //entropy estimator for accumulator
    //re-use the same entropy estimator for StreamingEntropy
    private EntropyEstimator streamEstimator;
    
    //logarithm base
    private String base;
    
    public Entropy() throws ExecException {
        //empirical estimator using Euler's number as logarithm base
        this(EntropyUtil.LOG);
    }
    
    public Entropy(String base) throws ExecException {
        try {
            this.streamEstimator = EntropyEstimator.createEstimator(EntropyEstimator.EMPIRICAL_ESTIMATOR, base);
        } catch (IllegalArgumentException ex) {
            throw new ExecException(
                    String.format("Fail to initialize Entropy with logarithm base: (%s), exception: (%s)", base, ex));
        }
        this.base = base;
    }
    
    /*
     * Algebraic implementation part
     */
    
    private String param = null;
    private String getParam()
    {
      if (param == null) {
        if (this.base != null) {
          param = String.format("('%s')", this.base);
        } else {
          param = "";
        }
      }
      return param;
    }
    
    @Override
    public String getFinal() {
        return Final.class.getName() + getParam();
    }

    @Override
    public String getInitial() {
       return Initial.class.getName() + getParam();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName() + getParam();
    }
    
    static public class Initial extends EvalFunc<Tuple> {
                
        public Initial(){}
        
        public Initial(String base){}
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            Tuple t = mTupleFactory.newTuple(2);

            try{
                //input is a bag with one tuple containing
                //the sample's occurrence frequency
                DataBag bg = (DataBag) input.get(0);
                Long cxl = null;
                if(bg.iterator().hasNext()) {
                   Tuple tp = bg.iterator().next();
                   cxl = getFreq(tp);
                }

                if(cxl == null || cxl.longValue() < 0) {
                    //invalid input frequency
                    t.set(0, null);
                    t.set(1, null);
                    warn("Non-positive input frequency number: " + cxl, PigWarning.UDF_WARNING_1);
                } else {
                    long cx = cxl.longValue();
                    double logcx = (cx > 0 ? Math.log(cx) : 0);
                    double cxlogcx = cx * logcx;
                    
                    //1st element of the returned tuple is freq * log(freq)
                    t.set(0, cxlogcx);
                    
                    //2nd element of the returned tuple is freq
                    t.set(1, cxl);
                }
                return t;
            } catch (NumberFormatException nfe) {
                //invalid input format
                //treat this input as null
                warn("Caught invalid format input number, exception: " + nfe, PigWarning.UDF_WARNING_2);
                try {
                    t.set(0, null);
                    t.set(1, null);
                } catch(ExecException e) {
                    throw e;
                }
                return t;
            } catch (ExecException ee) {
                throw ee;
            } catch(Exception e) {
                int errCode = 10080;
                String msg = "Error while computing entropy in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
            
        }
        
    }
    
    static public class Intermediate extends EvalFunc<Tuple> {
        
        public Intermediate(){}
        
        public Intermediate(String base){}
        
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                return combine(b);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 10081;
                String msg = "Error while computing entropy in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }
    
    static public class Final extends EvalFunc<Double> {
        private String base;
        
        public Final()
        {
            this(EntropyUtil.LOG);
        }
        
        public Final(String base)
        {
            this.base = base;
        }
        
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                Tuple combined = combine(b);
                
                Double sumOfCxLogCx = (Double)combined.get(0);
                Long sumOfCx = (Long)combined.get(1);
                
                if(sumOfCxLogCx == null || sumOfCx == null) {
                    return null;
                }
                
                Double entropy = null;
                
                double scxlogcx = sumOfCxLogCx.doubleValue();
                long scx = sumOfCx.longValue();
                                
                if (scx > 0) {
                    //H(X) = log(N) - 1 / N * SUM(c(x) * log(c(x)) )
                    entropy = EntropyUtil.logTransform(Math.log(scx) - scxlogcx / scx, this.base);
                }

                return entropy;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 10082;
                String msg = "Error while computing average in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }

        }        
    }
    
    static protected Tuple combine(DataBag values) throws ExecException { 
        Tuple output = mTupleFactory.newTuple(2);
        
        boolean sawNonNull = false;
        double sumOfCxLogCx = 0;
        long sumOfCx = 0;
        
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            Double scxlogcx = (Double)t.get(0);
            Long scx = (Long)t.get(1);
            
            sumOfCxLogCx += (scxlogcx == null ? 0 : scxlogcx.doubleValue());
            sumOfCx += (scx == null ? 0 : scx.longValue());
            
            if(scxlogcx != null && scx != null) {
                sawNonNull = true;
            }
        }
        
        if(sawNonNull) {
            output.set(0, sumOfCxLogCx);
            output.set(1, sumOfCx);
        } else {
            output.set(0, null);
            output.set(1, null);
        }
        return output;
    }
    
    static Long getFreq(Tuple tp) throws ExecException {
        Long cx = null;
        
        Object obj = tp.get(0);
        
        if(obj != null) {
            switch (tp.getType(0))
            {
            case DataType.LONG : cx = (Long)obj; break;
            case DataType.INTEGER: cx = ((Integer)obj).longValue(); break;
            case DataType.FLOAT: cx = ((Float)obj).longValue(); break;
            case DataType.DOUBLE: cx = ((Double)obj).longValue(); break;
            case DataType.BYTEARRAY: cx = Double.valueOf(((DataByteArray)obj).toString()).longValue(); break;
            case DataType.CHARARRAY: cx = Double.valueOf(obj.toString()).longValue(); break;
            default: 
            }
        }
                    
        return cx;
    }
    
    /*
     * Accumulator implementation part
     */

    @Override
    public void accumulate(Tuple input) throws IOException
    {
        for (Tuple t : (DataBag) input.get(0)) {
            long cx = getFreq(t);
            this.streamEstimator.accumulate(cx);
        }
    }

    @Override
    public Double getValue()
    {
      return streamEstimator.getEntropy();
    }

    @Override
    public void cleanup()
    {
      if(this.streamEstimator != null) {
         this.streamEstimator.reset();
      }
    }
    
    @Override
    public Schema outputSchema(Schema input)
    {
        try {
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
            
            Schema tupleSchema = inputBagSchema.getField(0).schema;
            
            if(tupleSchema == null) {
                throw new RuntimeException("The tuple of input bag has no schema");
            }
            
            List<Schema.FieldSchema> fieldSchemaList = tupleSchema.getFields();
            
            if(fieldSchemaList == null || fieldSchemaList.size() != 1) {
                throw new RuntimeException("The field schema of the input tuple is null or its size is not 1");
            }
            
            if(fieldSchemaList.get(0).type != DataType.BYTEARRAY &&
               fieldSchemaList.get(0).type != DataType.CHARARRAY &&
               fieldSchemaList.get(0).type != DataType.INTEGER &&
               fieldSchemaList.get(0).type != DataType.LONG &&
               fieldSchemaList.get(0).type != DataType.FLOAT &&
               fieldSchemaList.get(0).type != DataType.DOUBLE) {
                String[] expectedTypes = new String[] {DataType.findTypeName(DataType.BYTEARRAY),
                                                           DataType.findTypeName(DataType.CHARARRAY),
                                                           DataType.findTypeName(DataType.INTEGER),
                                                           DataType.findTypeName(DataType.LONG),
                                                           DataType.findTypeName(DataType.FLOAT),
                                                           DataType.findTypeName(DataType.DOUBLE)};
                throw new RuntimeException("Expect the type of the input tuple to be of (" +
                        java.util.Arrays.toString(expectedTypes) + "), but instead found " + 
                        DataType.findTypeName(fieldSchemaList.get(0).type));
            } 
            
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                                   .getName()
                                                                   .toLowerCase(), input),
                                                 DataType.DOUBLE));
          } catch (FrontendException e) {
            throw new RuntimeException(e);
          }
     }    
}
