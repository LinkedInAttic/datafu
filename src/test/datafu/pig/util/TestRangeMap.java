package datafu.pig.util;


import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class TestRangeMap
{
  @Test
  public void testGet()
  {
    RangeMap<Integer> rangeMap = new RangeMap(Arrays.asList(
        new SimpleEntry("[0,1)",8),
        new SimpleEntry("[1,3)",4),
        new SimpleEntry("[3,8)",2),
        new SimpleEntry("[8,*)",1)
        ));

    assertEquals(rangeMap.get(0.0).intValue(), 8);
    assertEquals(rangeMap.get(1.5).intValue(), 4);
    assertEquals(rangeMap.get(3).intValue(), 2);
    assertEquals(rangeMap.get(10).intValue(), 1);
    assertEquals(rangeMap.get(-5), null);
  }

  @Test
  public void testOverlapping()
  {
    try
    {
      RangeMap<Integer> rangeMap = new RangeMap(Arrays.asList(
          new SimpleEntry("[0,1]",8),
          new SimpleEntry("[1,3)",4)
          ));
      fail("illegal");
    }
    catch(IllegalArgumentException e)
    {

    }
  }
}
