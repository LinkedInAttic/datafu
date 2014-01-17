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

package datafu.pig.stats.entropy;

public class EntropyUtil {
    
    public static final String LOG = "log";
    
    public static final String LOG2 = "log2";
    
    public static final String LOG10 = "log10";

    /*
     * Transform the input entropy to that in the input logarithm base
     * The input entropy is assumed to be calculated in the Euler's number logarithm base
     * */
    public static double logTransform(double h, String base) {
        if(LOG2.equalsIgnoreCase(base)) {
            return h / Math.log(2);
        }

        if(LOG10.equalsIgnoreCase(base)) {
            return h / Math.log(10);
        }
        
        return h;
    }
    
    public static boolean isValidLogBase(String base) {
        return LOG.equalsIgnoreCase(base) ||
               LOG2.equalsIgnoreCase(base) ||
               LOG10.equalsIgnoreCase(base);
    }
}
