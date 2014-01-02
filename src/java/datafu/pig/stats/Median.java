/*
 * Copyright 2010 LinkedIn Corp. and contributors
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
 * Computes the {@link <a href="http://en.wikipedia.org/wiki/Median" target="_blank">median</a>} 
 * for a <b>sorted</b> input bag, using type R-2 estimation.  This is a convenience wrapper around Quantile.
 *
 * <p>
 * N.B., all the data is pushed to a single reducer per key, so make sure some partitioning is 
 * done (e.g., group by 'day') if the data is too large.  That is, this isn't distributed median.
 * </p>
 * 
 * @see Quantile
 */
public class Median extends Quantile
{
  public Median()
  {
    super("0.5");
  }
}
