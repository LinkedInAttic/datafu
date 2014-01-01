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

import org.apache.pig.backend.executionengine.ExecException;

import datafu.pig.stats.entropy.EntropyUtil;


/*
* Entropy estimator which exposes a unified interface to application
* and hides the implementation details in its subclasses
*/
public abstract class EntropyEstimator {

    public static final String EMPIRICAL_ESTIMATOR = "empirical";
    
    public static final String CHAOSHEN_ESTIMATOR = "chaosh";
    
    /*
     * logarithm base
     * by default, we use Euler's number as the default logarithm base
     */ 
    protected String base;
    
    protected EntropyEstimator(String base) throws IllegalArgumentException {
        if(!EntropyUtil.isValidLogBase(base)) {
            throw new IllegalArgumentException("Invalid input logarithm base. " + 
                    "Please refer to StreamingEntropy's javadoc for supported logarithm base");
        }
        this.base = base;
    }
    
    /* 
     * create estimator instance based on the input type
     * @param type  type of entropy estimator
     * @param base logarithm base
     * if the method is not supported, a null estimator instance is returned
     * and the external application needs to handle this case   
     */
    public static EntropyEstimator createEstimator(String type, 
                                                   String base) throws IllegalArgumentException 
    {
        //empirical estimator
        if(EmpiricalEntropyEstimator.EMPIRICAL_ESTIMATOR.equalsIgnoreCase(type)) {
            return new EmpiricalEntropyEstimator(base);
        }
        //chao-shen estimator
        if(EmpiricalEntropyEstimator.CHAOSHEN_ESTIMATOR.equalsIgnoreCase(type)) {
           return new ChaoShenEntropyEstimator(base);
        }
        //unsupported estimator type
        throw new IllegalArgumentException("invalid input entropy estimator type. " +
                    "Please refer to StreamingEntropy's javadoc for the supported estimator types");        
    }
    
    /*
     * accumulate occurrence frequency from input stream of samples
     * @param cx the occurrence frequency of the last input sample
     */
    public abstract void accumulate(long cx) throws ExecException;

    /*
     * calculate the output entropy value
     * return entropy value as a double type number
     */
    public abstract double getEntropy();

    /*
     * cleanup and reset internal states
     */
    public abstract void reset();
}
