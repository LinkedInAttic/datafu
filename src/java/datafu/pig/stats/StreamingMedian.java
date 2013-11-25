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

/**
 * Computes the approximate {@link <a href="http://en.wikipedia.org/wiki/Median" target="_blank">median</a>} 
 * for a (not necessarily sorted) input bag, using the Munro-Paterson algorithm.  
 * This is a convenience wrapper around StreamingQuantile.
 *
 * <p>
 * N.B., all the data is pushed to a single reducer per key, so make sure some partitioning is 
 * done (e.g., group by 'day') if the data is too large.  That is, this isn't distributed median.
 * </p>
 * 
 * @see StreamingQuantile
 */
public class StreamingMedian extends StreamingQuantile
{
  public StreamingMedian()
  {
    super("0.5");
  }
}
