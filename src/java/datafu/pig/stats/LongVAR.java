/*
 * Copyright 2012 LinkedIn Corp. and contributors
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

package datafu.pig.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.executionengine.ExecException;


/**
* Use {@link VAR} 
*/
public class LongVAR extends EvalFunc<Double> implements Algebraic, Accumulator<Double> {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            Long sum = sum(input);
            Long sumSquare = sumSquare(input);
            
            if(sum == null) {
                // either we were handed an empty bag or a bag
                // filled with nulls - return null in this case
                return null;
            }
            long count = count(input);

            Double var = null;
            if (count > 0){
                Double avg = new Double((double)sum / count);
                Double avgSquare = new Double((double)sumSquare / count);
                var = avgSquare - avg*avg;
            }
    
            return var;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            Tuple t = mTupleFactory.newTuple(3);
            try {
                // input is a bag with one tuple containing
                // the column we are trying to get variance 
                DataBag bg = (DataBag) input.get(0);
                Long l = null;
                Iterator<Tuple> iter = bg.iterator();
                if(iter.hasNext()) {
                    Tuple tp = iter.next();
                    l = (Long)tp.get(0);
                }
                
                if (iter.hasNext())
                {
                  throw new RuntimeException("Expected only one tuple in bag");
                }
                
                if (l == null) {
                    t.set(0, null);
                    t.set(1, null);
                    t.set(2, 0L);
                }
                else { 
                    t.set(0, l);
                    t.set(1, l*l);
                    t.set(2, 1L);
                }
                return t;
            } catch(NumberFormatException nfe) {
                nfe.printStackTrace();
                // invalid input,
                // treat this input as null
                try {
                    t.set(0, null);
                    t.set(1, null);
                    t.set(2, 0L);
                } catch (ExecException e) {
                    throw e;
                }
                return t;
            } catch (ExecException ee) {
                ee.printStackTrace();
                throw ee;
            } catch (Exception e) {
                e.printStackTrace();
                int errCode = 2106;
                String msg = "Error while computing variance in " + this.getClass().getSimpleName();
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
                int errCode = 2106;
                String msg = "Error while computing variacne in " + this.getClass().getSimpleName();
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

                Long sum = (Long)combined.get(0);
                Long sumSquare = (Long)combined.get(1);
                if(sum == null) {
                    return null;
                }
                Long count = (Long)combined.get(2);

                Double var = null;
                
                if (count > 0) {
                    Double avg = new Double((double)sum / count);
                    Double avgSquare = new Double((double)sumSquare / count);
                    var = avgSquare - avg*avg;
                }
                return var;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing variance in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    static protected Tuple combine(DataBag values) throws ExecException{
        long sum = 0;
        long sumSquare = 0;
        long totalCount = 0;

        // combine is called from Intermediate and Final
        // In either case, Initial would have been called
        // before and would have sent in valid tuples
        // Hence we don't need to check if incoming bag
        // is empty

        Tuple output = mTupleFactory.newTuple(3);
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            Long l = (Long)t.get(0);
            Long lSquare = (Long)t.get(1);
            Long count = (Long)t.get(2);
            
            // we count nulls in var as contributing 0
            // a departure from SQL for performance of
            // COUNT() which implemented by just inspecting
            // size of the bag
            if(l == null) {
                l = (long)0;
                lSquare = (long)0;
            } else {
                sawNonNull = true;
            }
            sum += l;
            sumSquare += lSquare;
            totalCount += count;
        }
        if(sawNonNull) {
            output.set(0, new Long(sum));
            output.set(1, new Long(sumSquare));
        } else {
            output.set(0, null);
            output.set(1, null);
        }
        output.set(2, Long.valueOf(totalCount));
        return output;
    }

    static protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        long cnt = 0;
        Iterator<Tuple> it = values.iterator();
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0 && t.get(0) != null)
                cnt ++;
        }
                    
        return cnt;
    }

    static protected Long sum(Tuple input) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        if(values.size() == 0) {
            return null;
        }

        long sum = 0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try{
                Long l = (Long)t.get(0);
                if (l == null) continue;
                sawNonNull = true;
                sum += l;
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of values.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if(sawNonNull) {
            return new Long(sum);
        } else {
            return null;
        }
    }
    
    static protected Long sumSquare(Tuple input) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        if(values.size() == 0) {
            return null;
        }

        long sumSquare = 0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try{
                Long l = (Long)t.get(0);
                if (l == null) continue;
                sawNonNull = true;
                sumSquare += l*l;
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of squared values.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if(sawNonNull) {
            return new Long(sumSquare);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }

    /* Accumulator interface implementation */
    private Long intermediateSumSquare = null;
    private Long intermediateSum = null;
    private Long intermediateCount = null;
    
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Long sum = sum(b);
            if(sum == null) {
                return;
            }
            
            Long sumSquare = sumSquare(b);
            if(sumSquare == null) {
                return;
            }
            
            // set default values
            if (intermediateSum == null || intermediateCount == null) {
                intermediateSumSquare = (long) 0;
                intermediateSum = (long) 0;
                intermediateCount = (long) 0;
            }
            
            long count = (Long)count(b);

            if (count > 0) {
                intermediateCount += count;
                intermediateSum += sum;
                intermediateSumSquare += sumSquare;
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing variance in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        intermediateSumSquare = null;
        intermediateSum = null;
        intermediateCount = null;
    }

    @Override
    public Double getValue() {
        Double var = null;
        if (intermediateCount != null && intermediateCount > 0) {
            Double avg = new Double((double)intermediateSum / intermediateCount);
            Double avgSquare = new Double((double)intermediateSumSquare / intermediateCount);
            var = avgSquare - avg*avg;
        }
        return var;
    }
}
