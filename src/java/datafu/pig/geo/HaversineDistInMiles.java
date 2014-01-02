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
 
package datafu.pig.geo;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Computes the distance (in miles) between two latitude-longitude pairs 
 * using the {@link <a href="http://en.wikipedia.org/wiki/Haversine_formula" target="_blank">Haversine formula</a>}.
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * -- input is a TSV of two latitude and longitude pairs
 * input = LOAD 'input' AS (lat1 : double, long1 : double, lat2 : double, long2 : double);
 * output = FOREACH input GENERATE datafu.pig.geo.HaversineDistInMiles(lat1, long1, lat2, long2) as distance;
 * }</pre></p>
 */
public class HaversineDistInMiles extends SimpleEvalFunc<Double>
{
  public static final double EARTH_RADIUS = 3958.75;

  public Double call(Double lat1, Double lng1, Double lat2, Double lng2)
  {
    if (lat1 == null || lng1 == null || lat2 == null || lng2 == null)
      return null;

    double d_lat = Math.toRadians(lat2-lat1);
    double d_long = Math.toRadians(lng2-lng1);
    double a = Math.sin(d_lat/2) * Math.sin(d_lat/2) +
               Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
               Math.sin(d_long/2) * Math.sin(d_long/2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return EARTH_RADIUS * c;
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    return new Schema(new Schema.FieldSchema("dist", DataType.DOUBLE));
  }
}
