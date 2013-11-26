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

package datafu.test.pig.geo;

import static org.testng.Assert.*;

import java.util.List;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class GeoTests extends PigTests
{
  /**
  register $JAR_PATH

  define HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles();
  
  data = LOAD 'input' AS (lat1:double,lng1:double,lat2:double,lng2:double);
  
  data2 = FOREACH data GENERATE HaversineDistInMiles(lat1,lng1,lat2,lng2);
  
  STORE data2 INTO 'output';
   */
  @Multiline
  private String haversineTest;
  
  @Test
  public void haversineTest() throws Exception
  {    
    PigTest test = createPigTestFromString(haversineTest);
    
    // Approximate latitude and longitude for major cities from maps.google.com
    double[] la = {34.040143,-118.243103};
    double[] tokyo = {35.637209,139.65271};
    double[] ny = {40.716038,-73.99498};
    double[] paris = {48.857713,2.342491};
    double[] sydney = {-33.872696,151.195221};
        
    this.writeLinesToFile("input", 
                          coords(la,tokyo),
                          coords(ny,tokyo),
                          coords(ny,sydney),
                          coords(ny,paris));
    
    test.runScript();
    
    List<Tuple> distances = this.getLinesForAlias(test, "data2");
    
    // ensure distance is within 20 miles of expected (distances found online)
    assertWithin(5478.0, distances.get(0), 20.0); // la <-> tokyo
    assertWithin(6760.0, distances.get(1), 20.0); // ny <-> tokyo
    assertWithin(9935.0, distances.get(2), 20.0); // ny <-> sydney
    assertWithin(3635.0, distances.get(3), 20.0); // ny <-> paris
    
  }
  
  private void assertWithin(double expected, Tuple actual, double maxDiff) throws Exception
  {
    Double actualVal = (Double)actual.get(0);
    assertTrue(Math.abs(expected-actualVal) < maxDiff);
  }
  
  private String coords(double[] coords1, double[] coords2)
  {
    assertTrue(coords1.length == 2);
    assertTrue(coords2.length == 2);
    return String.format("%f\t%f\t%f\t%f", coords1[0], coords1[1], coords2[0], coords2[1]);
  }
}
