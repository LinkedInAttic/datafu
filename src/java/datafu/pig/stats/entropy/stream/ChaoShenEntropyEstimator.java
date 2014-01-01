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

package datafu.pig.stats.entropy.stream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;

import java.util.Collections;

import datafu.pig.stats.entropy.EntropyUtil;


/*
 * This class implements the Chao-Shen entropy estimator based on this  
 * {@link <a href="http://www.researchgate.net/publication/226041346_Nonparametric_estimation_of_Shannons_index_of_diversity_when_there_are_unseen_species_in_sample/file/d912f510a79b8aae74.pdf" target="_blank">paper</a>} 
 * <p>
 * It combines the {@link <a href="http://en.wikipedia.org/wiki/Horvitz%E2%80%93Thompson_estimator" target="_blank">Horvitz-Thompson estimator</a>}
 * to adjust for missing species and the concept of sample coverage to estimate the relative abundances of species in the sample. 
 * The entropy calculation formula is as follows:
 * <pre>
 * {@code
 *   H(X) = - SUM( ( C * c(x) / N * log(C * c(x) / N) ) / ( 1 - (1 - C * c(x) / N) ^ N ) )
 *   c(x) is the occurrence frequency of sample x, N is the sum of c(x)
 *   C = 1 - N1 / N, N1 is the number of samples whose c(x) equals 1.
 * }
 * </pre>
 * </p>
 */
class ChaoShenEntropyEstimator extends EntropyEstimator {
    //sample frequency 
    private AccumulativeSampleFrequencyMap freqBaggedMap;
    
    //sum of frequency of all samples
    private long N;
    
    //number of samples whose occurrence frequency is 1
    private long N1;
    
    ChaoShenEntropyEstimator(String base){
        super(base);
        reset();
    }

    @Override
    public void accumulate(long cx) throws ExecException {
        if(cx > 0) {
            freqBaggedMap.accumulate(cx);  
            N += cx;
            if(cx == 1) {
               N1++;
            }
        }
    }

    @Override
    public double getEntropy() {
        double h = 0.0;
        
        if(N > 0) {

            if(N1 == N) {
               //avoid c == 0
               N1 = N - 1;
            }

            //sample coverage estimation
            double c = 1 - (double)N1 / N;
            
            try {
                 //from in-memory frequency map
                 for(Iterator<Map.Entry<Long, MutableLong>> iter = this.freqBaggedMap.getInternalMap().entrySet().iterator();
                         iter.hasNext(); ) {
                     Map.Entry<Long, MutableLong> entry = iter.next();
                     long cx = entry.getKey();
                     long cnt = entry.getValue().longValue();
                     h += accumlateEntropy(cx, this.N, c, cnt);
                 }
                 //from backup databag
                 for(Iterator<Tuple> iter = this.freqBaggedMap.getInternalBag().iterator();
                          iter.hasNext(); ) {
                    Tuple t = iter.next();
                    long cx = (Long)t.get(0);
                    long cnt = (Long)t.get(1);
                    h += accumlateEntropy(cx, this.N, c, cnt);
                 }
            } catch(ExecException ex) {
                 throw new RuntimeException(
                         "Error while computing chao-shen entropy, exception: " + ex);
            }
        }

        return EntropyUtil.logTransform(h, super.base);
    }
    
    private double accumlateEntropy(long cx, 
                                    long N,
                                    double c,
                                    long cnt) {
        //sample proportion
        double p = (double)cx / N;
        //sample coverage adjusted probability
        double pa = c * p;
        //probability to see an individual in the sample
        double la = 1 - Math.pow((1 - pa), N);
        return -(cnt * pa * Math.log(pa) / la);
    }

    @Override
    public void reset() {
        this.N = 0;
        this.N1 = 0;
        if(this.freqBaggedMap != null) {
            this.freqBaggedMap.clear();
        }
        this.freqBaggedMap = new AccumulativeSampleFrequencyMap();
    }
    
    /*
     * A map backed by a data bag to record the sample occurrence frequencies which might be repeated
     * The purpose of this class is that we suspect in the real applications, the sample occurrence frequency
     * might be repeated and putting all these repeated numbers into databag may be space inefficient
     * So we have an in-memory map to record the repetitions and use a databag to backup when the size of the
     * in-memory map exceeds a pre-defined threshold. At present we use 5M as the default threshold to control
     * the number of entries in the in-memory map, which approximates to 80M bytes. 
     */
    private class AccumulativeSampleFrequencyMap {
        
        private long spillBytesThreshold;
        
        /* key is the sample occurrence frequency
         * value is the number of repetitions of this frequency
         */
        private Map<Long, MutableLong> countMap;
        
        /* the backed databag
         * each tuple has 2 elements
         * the 1st element is sample occurrence frequency
         * the 2nd element is the number of repetitions
         */
        private DataBag countBag;
           
        AccumulativeSampleFrequencyMap(){
            this(1024 * 1024 * 5 * (Long.SIZE + Long.SIZE) / 8);
        }
        
        AccumulativeSampleFrequencyMap(long spillBytesThreshold) {
            this.spillBytesThreshold = spillBytesThreshold;
            clear();
        }
        
        void accumulate(long key) throws ExecException {
            MutableLong val = this.countMap.get(key);
            if(val == null) {
                this.countMap.put(key, new MutableLong(1));
            } else {
                val.add(1);
            }
            
            if(this.countMap.size() * (Long.SIZE + Long.SIZE) / 8 > spillBytesThreshold) {
                spillFromMap2Bag();
            }
        }
        
        private void spillFromMap2Bag() throws ExecException {
            for(Map.Entry<Long, MutableLong> entry : this.countMap.entrySet()) {
                Tuple t = TupleFactory.getInstance().newTuple(2);
                t.set(0, entry.getKey());
                t.set(1, entry.getValue().longValue());
                this.countBag.add(t);
            }
            this.countMap.clear();
        }
        
        Map<Long, MutableLong> getInternalMap() {
            return Collections.unmodifiableMap(this.countMap);
        }
        
        DataBag getInternalBag() {
            return this.countBag;
        }
        
        void clear() {
            countMap = new HashMap<Long, MutableLong>();
            countBag = BagFactory.getInstance().newDefaultBag();
        }
    }
    
    private class MutableLong {
        private long val;
        
        MutableLong(){
            this(0L);
        }
        
        MutableLong(long val) {
            this.val = val;
        }
        
        long longValue(){
            return val;
        }
        
        void add(long additive){
            this.val += additive;
        }
    }
}
