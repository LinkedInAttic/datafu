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

package datafu.test.pig.stats.entropy;

import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.Tuple;

import org.apache.pig.backend.executionengine.ExecException;

import datafu.test.pig.PigTests;

public abstract class AbstractEntropyTests extends PigTests
{
  protected void verifyEqualEntropyOutput(List<Double> expectedOutput, List<Tuple> output, int digits) throws ExecException {
    assertEquals(expectedOutput.size(), output.size());
    Iterator<Double> expectationIterator = expectedOutput.iterator();
    String formatDigits = "%." + digits + "f";
    for (Tuple t : output)
    {
      Double entropy = (Double)t.get(0);
      assertEquals(String.format(formatDigits,entropy),String.format(formatDigits, expectationIterator.next()));
    }
  }
}
