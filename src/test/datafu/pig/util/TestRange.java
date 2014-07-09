package datafu.pig.util;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestRange
{
  @Test
  public void testIntegerBuckets()
  {
    NumericalRange range = NumericalRange.fromRangeString("[-10, 10)");
    assertEquals(range.isInRange(-10), true);
    assertEquals(range.isInRange(9.99), true);
    assertEquals(range.isInRange(10), false);
  }

  @Test
  public void testFloatingBuckets()
  {
    NumericalRange range = NumericalRange.fromRangeString("[-10.5, 10.0)");
    assertEquals(range.isInRange(-10), true);
  }

  @Test
  public void testScientificBuckets()
  {
    NumericalRange range = NumericalRange.fromRangeString("[-1.5e-9, 0]");
    assertEquals(range.isInRange(-1.5e-9), true);
    assertEquals(range.isInRange(-1e-8), false);
  }

  @Test
  public void testNegInfinity()
  {
    NumericalRange range = NumericalRange.fromRangeString("(*,1)");
    assertEquals(range.isInRange(-1e10), true);
    assertEquals(range.isInRange(0), true);
    assertEquals(range.isInRange(1), false);
  }

  @Test
  public void testInfinity()
  {
    NumericalRange range = NumericalRange.fromRangeString("[0,*)");
    assertEquals(range.isInRange(0), false);
    assertEquals(range.isInRange(1e10), true);
  }

  @Test
  public void testMalformedBuckets1()
  {
    try
    {
      NumericalRange range = NumericalRange.fromRangeString("[-10 10)");
      fail("invalid");
    }
    catch(IllegalArgumentException e)
    {

    }
  }

  @Test
  public void testMalformedBuckets2()
  {
    try
    {
      NumericalRange range = NumericalRange.fromRangeString("[-10 IMANUMBER)");
      fail("invalid");
    }
    catch(IllegalArgumentException e)
    {

    }
  }
}
