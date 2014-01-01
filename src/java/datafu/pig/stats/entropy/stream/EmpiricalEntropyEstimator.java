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

import datafu.pig.stats.entropy.EntropyUtil;


/*
 * This entropy estimator calculates the empirical entropy of given species
 * using the occurrence frequency of individual in the sample as an estimation of its probability. 
 * The empirical estimator is also called the maximum likelihood estimator (MLE). 
 * <p>
 * It applies the following formula to compute entropy:
 * <pre>
 * {@code
 * H(X) = log(N) - 1 / N * SUM(c(x) * log(c(x)) )
 * c(x) is the occurrence frequency of sample x, N is the sum of c(x)
 * }
 * </pre>
 * </p>
 * <p>
 * This entropy estimator is widely used when the number of species is known and small.
 * But it is biased since it does not cover the rare species with zero frequency in the sample.
 * </p>
 */
class EmpiricalEntropyEstimator extends EntropyEstimator {
    
    //sum of frequency cx of all input samples
    private long N;
    
    //sum of cx * log(cx) of all input samples
    private double M;
    
    EmpiricalEntropyEstimator(String base) throws IllegalArgumentException {
        super(base);
        reset();
    }

    @Override
    public void accumulate(long cx) {
        if(cx > 0) {
           N += cx;
           M += cx * Math.log(cx);
        }
    }

    @Override
    public double getEntropy() {
        return N > 0 ? EntropyUtil.logTransform(Math.log(N) - M / N, super.base) : 0;
    }
    
    @Override
    public void reset(){
        N = 0;
        M = 0;
    }
}
