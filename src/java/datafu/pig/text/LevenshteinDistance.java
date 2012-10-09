/*
 * Copyright 2012 LinkedIn, Inc
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

package datafu.pig.text;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Calculates Levenshtein distance between two strings.
 * e.g. distance bewteen cat and cot = 1
 *
 * LevenshteinDistance may be passed a maximum distance parameter k.  
 * This reduces the compute time from len(string1) * len(string2) to len(string) * k;
 * Example:
 *
 * <pre>
 * {@code
 * 
 * define LevenshteinDistance datafu.pig.text.LevenshteinDistance('10');
 * 
 * data = load 'input' as (str1:chararray, str2:chararray);
 * data_out = FOREACH data GENERATE LevenshteinDistance(str1, str2) as distance;
 * store data_out into 'output';
 * 
 * }
 * </pre>
 * 
 */
public class LevenshteinDistance extends SimpleEvalFunc<Integer> 
{
  private Integer maxDistance;
  private HashMap cache;

  public LevenshteinDistance(String... parameters) {
    if (parameters.length > 1) {
      throw new RuntimeException("Too many parameters");
    }
    else {
      try {
        this.maxDistance = Integer.parseInt(parameters[0]);
      }
      catch (Exception e) {
        throw new RuntimeException("Incorrect Parameter value");
      }
    }
  }

  public LevenshteinDistance() {
    this.maxDistance = Integer.MAX_VALUE;
  }

  public Integer call(String word1, String word2) throws IOException
  {
    try
    {
      //TODO: consider possibility of keeping memoize cache between calls; would need way to clean out cache if gets big
      cache = new HashMap();  
      return distance(word1, word2, 0);
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Integer distance(String s, String t, Integer entryCost) {
    if ((entryCost != null) && (entryCost >= this.maxDistance)) return 0;

    //Check memoization cache
    Integer dist = (Integer)cache.get(s + "_" + t);
    if (dist == null) dist = (Integer)cache.get(t + "_" + s);
    if (dist == null) {

      int len_s = s.length();
      int len_t = t.length();

      if (len_s == 0) dist = len_t;
      else if (len_t == 0) dist = len_s;
      else {
        int cost = 0;
        if (s.charAt(0) != t.charAt(0)) cost = 1;

        dist = Math.min(distance(s.substring(1), t, entryCost + 1) + 1,
                        Math.min(distance(s, t.substring(1), entryCost + 1) + 1,
                                 distance(s.substring(1), t.substring(1), entryCost + cost) + cost));
      }
      
      //Place the element into the memoization cache if we were not cut off by the maxDistance parameter
      if ((dist + entryCost) < this.maxDistance)
        cache.put(s + "_" + t, dist);
    }
    return dist;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema(new Schema.FieldSchema("distance", DataType.INTEGER));
  }
  
}

