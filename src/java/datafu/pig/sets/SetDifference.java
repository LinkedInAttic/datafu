/**
 * Copyright 2013 LinkedIn, Inc
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

/**
 * Computes the set difference of two or more bags.  Duplicates are eliminated. <b>The input bags must be sorted.</b>
 * 
 * <p>
 * If bags A and B are provided, then this computes A-B, i.e. all elements in A that are not in B.
 * If bags A, B and C are provided, then this computes A-B-C, i.e. all elements in A that are not in B or C.
 * </p>
 * 
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SetDifference datafu.pig.sets.SetDifference();
 *
 * -- input:
 * -- ({(1),(2),(3),(4),(5),(6)},{(3),(4)})
 * input = LOAD 'input' AS (B1:bag{T:tuple(val:int)},B2:bag{T:tuple(val:int)});
 *
 * input = FOREACH input {
 *   B1 = ORDER B1 BY val ASC;
 *   B2 = ORDER B2 BY val ASC;
 *
 *   -- output:
 *   -- ({(1),(2),(5),(6)})
 *   GENERATE SetDifference(B1,B2);
 * }
 * }</pre>
 */
public class SetDifference extends SetOperationsBase
{
  private static final BagFactory bagFactory = BagFactory.getInstance();
  
  /**
   * Loads the data bags from the input tuple and puts them in a priority queue,
   * where ordering is determined by the data from the iterator for each bag.
   * 
   * <p>
   * The bags are wrapped in a {@link Pair} object that is comparable on the data
   * currently available from the iterator.
   * These objects are ordered first by the data, then by the index within the tuple
   * the bag came from.
   * </p>
   *  
   * @param input
   * @return
   * @throws IOException
   */
  private PriorityQueue<Pair> loadBags(Tuple input) throws IOException
  {    
    PriorityQueue<Pair> pq = new PriorityQueue<Pair>(input.size());

    for (int i=0; i < input.size(); i++) 
    {
      if (input.get(i) != null)
      {
        Iterator<Tuple> inputIterator = ((DataBag)input.get(i)).iterator();      
        if(inputIterator.hasNext())
        {
          pq.add(new Pair(inputIterator,i));
        }
      }
    }
    return pq;
  }

  /**
   * Counts how many elements in the priority queue match the
   * element at the front of the queue, which should be from the first bag. 
   * 
   * @param pq priority queue
   * @return number of matches
   */
  public int countMatches(PriorityQueue<Pair> pq)
  {
    Pair nextPair = pq.peek();
    Tuple data = nextPair.data;
    
    // sanity check
    if (!nextPair.index.equals(0))
    {
      throw new RuntimeException("Expected next bag to have index 0");
    }
    
    int matches = 0;
    for (Pair p : pq) {
      if (data.equals(p.data))
        matches++;
    }
    // subtract 1 since element matches itself
    return matches - 1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    if (input.size() < 2)
    {
      throw new RuntimeException("Expected at least two inputs, but found " + input.size());
    }
    
    for (Object o : input)
    {
      if (o != null && !(o instanceof DataBag))
      {
        throw new RuntimeException("Inputs must be bags");
      }
    }

    DataBag outputBag = bagFactory.newDefaultBag();
    
    DataBag bag1 = (DataBag)input.get(0);
    DataBag bag2 = (DataBag)input.get(1);
    
    if (bag1 == null || bag1.size() == 0)
    {
      return outputBag;
    }
    // optimization
    else if (input.size() == 2 && (bag2 == null || bag2.size() == 0))
    {
      return bag1;
    }
    
    PriorityQueue<Pair> pq = loadBags(input);
    
    Tuple lastData = null;

    while (true) 
    {
      Pair nextPair = pq.peek();
      
      // ignore data we've already encountered
      if (nextPair.data.compareTo(lastData) != 0)
      {
        // Only take data from the first bag, where there are no other
        // bags that have the same data.
        if (nextPair.index.equals(0) && countMatches(pq) == 0)
        {
          outputBag.add(nextPair.data);
          lastData = nextPair.data;
        }
      }

      Pair p = pq.poll();      
      
      // only put the bag back into the queue if it still has data
      if (p.hasNext())
      {
        p.next();
        pq.offer(p);
      }
      else if (p.index.equals(0))
      {
        // stop when we exhaust all elements from the first bag
        break;
      }
    }

    return outputBag;
  }

  /**
   * A wrapper for the tuple iterator that implements comparable so it can be used in the priority queue.
   * 
   * <p>
   * This is compared first on the data, then on the index the bag came from
   * in the input tuple.
   * </p>
   * @author mhayes
   *
   */
  private static class Pair implements Comparable<Pair>
  {
    private final Iterator<Tuple> it;
    private final Integer index;
    private Tuple data;

    /**
     * Constructs the {@link Pair}.
     * 
     * @param it tuple iterator
     * @param index index within the tuple that the bag came from
     */
    public Pair(Iterator<Tuple> it, int index)
    {
      this.index = index;
      this.it = it;
      this.data = it.next();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Pair o)
    {
      int r = this.data.compareTo(o.data);
      if (r == 0)
      {
        return index.compareTo(o.index);
      }
      else
      {
        return r;
      }
    }
    
    public boolean hasNext()
    {
      return it.hasNext();
    }
    
    @SuppressWarnings("unchecked")
    public Tuple next()
    {
      Tuple nextData = it.next();
      // algorithm assumes data is in order
      if (data.compareTo(nextData) > 0)
      {
        throw new RuntimeException("Out of order!");
      }
      this.data = nextData;
      return this.data;
    }

    @Override
    public String toString()
    {
      return String.format("[%s within %d]",data,index);
    }
  }
}


