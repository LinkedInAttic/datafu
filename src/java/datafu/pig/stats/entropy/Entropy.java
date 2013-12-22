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


/**
 * Calculate entropy given a bag of sample counts following entropy's definition in 
 * {@link <a href="http://en.wikipedia.org/wiki/Entropy_%28information_theory%29" target="_blank">wiki</a>} 
 * 
 * <p>
 * This UDF extends * {@link org.apache.pig.AccumulatorEvalFunc} and implements 
 * * {@link org.apache.pig.Algebraic} so it could calculate entropy either in a streaming way 
 * or in a distributed fashion levering combiner
 * </p>
 * 
 * <p>
 * It uses empirical estimation that is the same as * {@link datafu.pig.stats.entropy.stream.EmpiricalEntropyEstimator} 
 * It uses Euler's number as the logarithm base so to get entropy in different logarithm base such as 2 or 10, the external application
 * could divide the result entropy by LOG(2) or LOG(10) 
 * </p>
 * 
 * <p>
 * Note: 
 * <ul>
 *     <li>the input to the UDF is a bag of each sample's occurrence frequency, 
 *     which is different from * {@link datafu.pig.stats.entropy.stream.StreamingEntropy}, 
 *     whose input is a sorted bag of raw data samples.
 *     <li>each tuple in the UDF's input bag could be int, long, float, double, chararray, bytearray
 *     The UDF will try to convert the input tuple's value to long type number.
 *     <li>the returned entropy value is of double type.
 * </ul>
 * </p>
 *
 * <p>
 * How to use: This UDF is suitable to calculate entropy in the whole data set when we 
 * could easily get the frequency of each sample's value using an outer GROUP BY. Then use another GROUP BY
 * on the frequency set to get entropy. Example:
 * <pre>
 * {@code
 * 
 * 
 * define Entropy datafu.pig.stats.entropy.Entropy();
 *
 * input = LOAD 'input' AS (val: double);
 *
 * -- calculate the occurrence of each sample
 * counts_g = GROUP input BY val;
 * counts = FOREACh counts_g GENERATE COUNT(input) as cnt;
 * 
 * -- calculate entropy 
 * input_counts_g = GROUP counts ALL;
 * entropy = FOREACH input_counts_g GENERATE Entropy(counts) AS entropy; 
 *
 * </pre>
 * </p>
 */

public class Entropy extends AccumulatorEvalFunc<Double> implements Algebraic {
    
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    //entropy estimator for accumulator
    //re-use the same entropy estimator for StreamingEntropy
    private EntropyEstimator streamEstimator;
    
    public Entropy() {
        //empirical estimator using Euler's number as logarithm base
        streamEstimator = EntropyEstimator.createEstimator(EntropyEstimator.EMPIRICAL_ESTIMATOR, "");
    }
    
    /*
     * Algebraic implementation part
     */
    
    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    @Override
    public String getInitial() {
       return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();
    }
    
    static public class Initial extends EvalFunc<Tuple> {
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
                    entropy = Math.log(scx) - scxlogcx / scx;
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
