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

package datafu.pig.sets;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Computes the set intersection of two or more bags.  Duplicates are eliminated. <b>The input bags must be sorted.</b>
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SetIntersect datafu.pig.sets.SetIntersect();
 *
 * -- input:
 * -- ({(1,10),(2,20),(3,30),(4,40)},{(2,20),(4,40),(8,80)})
 * input = LOAD 'input' AS (B1:bag{T:tuple(val1:int,val2:int)},B2:bag{T:tuple(val1:int,val2:int)});
 *
 * input = FOREACH input {
 *   B1 = ORDER B1 BY val1 ASC, val2 ASC;
 *   B2 = ORDER B2 BY val1 ASC, val2 ASC;
 *
 *   -- output:
 *   -- ({(2,20),(4,40)})
 *   GENERATE SetIntersect(B1,B2);
 * }
 * }</pre>
 */
public class SetIntersect extends SetOperationsBase
{
  private static final BagFactory bagFactory = BagFactory.getInstance();

  static class pair implements Comparable<pair>
  {
    final Iterator<Tuple> it;
    Tuple data;

    pair(Iterator<Tuple> it)
    {
      this.it = it;
      this.data = it.next();
    }

    @Override
    public int compareTo(pair o)
    {
      return this.data.compareTo(o.data);
    }
  }

  private PriorityQueue<pair> load_bags(Tuple input) throws IOException
  {
    PriorityQueue<pair> pq = new PriorityQueue<pair>(input.size());

    for (int i=0; i < input.size(); i++) {
      Object o = input.get(i);
      if (!(o instanceof DataBag))
        throw new RuntimeException("parameters must be databags");
      Iterator<Tuple> inputIterator= ((DataBag) o).iterator();
      if(inputIterator.hasNext())
        pq.add(new pair(inputIterator));
    }
    return pq;
  }

  public boolean all_equal(PriorityQueue<pair> pq)
  {
    Object o = pq.peek().data;
    for (pair p : pq) {
      if (!o.equals(p.data))
        return false;
    }
    return true;
  }

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    DataBag outputBag = bagFactory.newDefaultBag();
    PriorityQueue<pair> pq = load_bags(input);
    if(pq.size() != input.size())
      return outputBag; // one or more input bags were empty
    Tuple last_data = null;

    while (true) {
      if (pq.peek().data.compareTo(last_data) != 0 && all_equal(pq)) {
        last_data = pq.peek().data;
        outputBag.add(last_data);
      }

      pair p = pq.poll();
      if (!p.it.hasNext())
        break;
      Tuple nextData = p.it.next();
      // algorithm assumes data is in order
      if (p.data.compareTo(nextData) > 0)
      {
        throw new RuntimeException("Out of order!");
      }
      p.data = nextData;
      pq.offer(p);
    }

    return outputBag;
  }
}


