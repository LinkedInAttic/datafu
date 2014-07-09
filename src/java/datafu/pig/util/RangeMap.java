package datafu.pig.util;


import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * Representing a mapping from a range to any value. The result of calling get() on any number will be to find
 * the value associated with the range containing that number
 * @param <V>
 */
public class RangeMap<V> implements Map<Double, V>
{
  private final TreeMap<NumericalRange, V> treeMap;

  public RangeMap(List<Entry<String, V>> rangeStrings)
  {
    List<Entry<NumericalRange, V>> ranges = new ArrayList<Entry<NumericalRange, V>>(rangeStrings.size());
    for(Entry<String, V> rangeString : rangeStrings)
    {
      ranges.add(new AbstractMap.SimpleEntry<NumericalRange, V>(NumericalRange.fromRangeString(rangeString.getKey()), rangeString.getValue()));
    }

    Collections.sort(ranges, new Comparator<Entry<NumericalRange, V>>()
    {
      @Override
      public int compare(Entry<NumericalRange, V> entry1, Entry<NumericalRange, V> entry2)
      {
        return entry1.getKey().compareTo(entry2.getKey());
      }
    });

    for(int i = 1; i < ranges.size(); i++)
    {
      if(ranges.get(i).getKey().overlaps(ranges.get(i-1).getKey()))
      {
        throw new IllegalArgumentException("Ranges should not overlap, but found " + ranges.get(i) + " and " + ranges.get(i-1));
      }
    }

    TreeMap<NumericalRange, V> map = new TreeMap<NumericalRange, V>();
    for(Entry<NumericalRange, V> entry : ranges)
    {
      map.put(entry.getKey(), entry.getValue());
    }

    treeMap = map;
  }

  @Override
  public int size()
  {
    return treeMap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return treeMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object o)
  {
    if(o instanceof Number)
    {
      Number num = (Number) o;
      NumericalRange numRange = new NumericalRange(num.doubleValue(), num.doubleValue(), true, true);

      Entry<NumericalRange, V> floorEntry = treeMap.floorEntry(numRange);
      if(floorEntry == null)
      {
        floorEntry = treeMap.firstEntry();
      }

      if(floorEntry.getKey().overlaps(numRange))
      {
        return true;
      }
      else
      {
        Entry<NumericalRange, V> nextEntry = treeMap.higherEntry(floorEntry.getKey());
        if(nextEntry != null && nextEntry.getKey().overlaps(numRange))
        {
          return true;
        }
      }
      return false;
    }
    else
    {
      return false;
    }
  }


  @Override
  public boolean containsValue(Object o)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public V get(Object o)
  {
    if(o instanceof Number)
    {
      Number num = (Number) o;
      NumericalRange numRange = new NumericalRange(num.doubleValue(), num.doubleValue(), true, true);

      Entry<NumericalRange, V> floorEntry = treeMap.floorEntry(numRange);
      if(floorEntry == null)
      {
        floorEntry = treeMap.firstEntry();
      }

      if(floorEntry.getKey().overlaps(numRange))
      {
        return floorEntry.getValue();
      }
      else
      {
        Entry<NumericalRange, V> nextEntry = treeMap.higherEntry(floorEntry.getKey());
        if(nextEntry != null && nextEntry.getKey().overlaps(numRange))
        {
          return nextEntry.getValue();
        }
      }
      return null;
    }
    else
    {
      return null;
    }
  }

  @Override
  public V put(Double aDouble, V v)
  {
    throw new UnsupportedOperationException("The range map is immutable");
  }

  @Override
  public V remove(Object o)
  {
    throw new UnsupportedOperationException("The range map is immutable");
  }

  @Override
  public void putAll(Map<? extends Double, ? extends V> map)
  {
    throw new UnsupportedOperationException("The range map is immutable");
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException("The range map is immutable");
  }

  @Override
  public Set<Double> keySet()
  {
    throw new UnsupportedOperationException("Listing all possible numbers does not make sense");
  }

  @Override
  public Collection<V> values()
  {
    return treeMap.values();
  }

  @Override
  public Set<Entry<Double, V>> entrySet()
  {
    throw new UnsupportedOperationException("Listing all possible numbers does not make sense");
  }
}
