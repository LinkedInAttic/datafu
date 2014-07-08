package datafu.pig.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class represents a range of numbers, including whether or not the range is open or closed
 *
 * It has utility methods to parse numerical ranges using standard mathematical notation. For instance:
 * [-1e-9,10)
 *
 * It also has support for infinite ranges through the following syntax
 * [-10,*)
 *
 * Single points are also supported:
 * 5
 *
 * As are entirely open ranges: *
 *
 * @author jhartman
 * @author rugupta
 */
public class NumericalRange implements Comparable<NumericalRange>
{
  private static final String DOUBLE_REGEX = "-?\\d+(\\.\\d+)?";
  private static final String NUMBER_REGEX = DOUBLE_REGEX + "(" + "[eE]" + DOUBLE_REGEX + ")?";
  private static final String NUMBER_OR_STAR_REGEX = "(" + NUMBER_REGEX + ")" + "|\\*";
  private static final Pattern NUMBER_OR_STAR_PATTERN = Pattern.compile(NUMBER_OR_STAR_REGEX);

  private static final String LOWER_REGEX = "[\\(\\[]" + "(" + NUMBER_OR_STAR_REGEX + ")" + "\\s*,\\s*";
  private static final String UPPER_REGEX = "(" + NUMBER_OR_STAR_REGEX + ")" + "\\s*[\\)\\]]";

  private static final Pattern LOWER_PATTERN = Pattern.compile(LOWER_REGEX);
  private static final Pattern UPPER_PATTERN = Pattern.compile(UPPER_REGEX);

  public static final String RANGE_REGEX = LOWER_REGEX + UPPER_REGEX;
  public static final Pattern RANGE_PATTERN = Pattern.compile(RANGE_REGEX);

  private final double lower;
  private final double upper;
  private final boolean lowerClosed;
  private final boolean upperClosed;

  public NumericalRange(double lower, double upper, boolean lowerClosed, boolean upperClosed)
  {
    this.lower = lower;
    this.upper = upper;
    this.lowerClosed = lowerClosed;
    this.upperClosed = upperClosed;
  }

  public double getLower()
  {
    return lower;
  }

  public double getUpper()
  {
    return upper;
  }

  public boolean getLowerClosed()
  {
    return lowerClosed;
  }

  public boolean getUpperClosed()
  {
    return upperClosed;
  }

  public static NumericalRange fromRangeString(String range)
  {
    range = range.trim();
    if(NUMBER_OR_STAR_PATTERN.matcher(range).matches())
    {
      if("*".equals(range))
      {
        return new NumericalRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true, false);
      }
      else
      {
        double point = Double.parseDouble(range);
        return new NumericalRange(point, point, true, true);
      }
    }
    else if(RANGE_PATTERN.matcher(range).matches())
    {
      double lower = handleRange(range, true);
      double upper = handleRange(range, false);
      if (lower > upper)
      {
        throw new IllegalArgumentException("Malformed range string: " + range);
      }

      return new NumericalRange(lower, upper, isClosed(range, false), isClosed(range, true));
    }
    else
    {
      throw new IllegalArgumentException("Malformed range. Expected a point or a range, but received " + range);
    }
  }

  private static boolean isClosed(String range, boolean upper)
  {
    if (upper)
    {
      return (range.charAt(range.length() - 1) == ']' ? true : false);
    }
    else
    {
      return (range.charAt(0) == '[' ? true : false);
    }
  }

  @Override
  public String toString()
  {
    return (lowerClosed ? "[" : "(") + lower + "," + upper + (upperClosed ? "]" : ")");
  }

  public boolean isInRange(double value)
  {
    boolean aboveLower = lowerClosed ? value >= lower : value > lower;

    if(aboveLower)
    {
      boolean belowUpper = upperClosed ? value <= upper : value < upper;
      return belowUpper;
    }
    else
    {
      return false;
    }
  }

  /**
   * Determines whether or not two ranges are overlapping
   * @param that
   * @return
   */
  public boolean overlaps(NumericalRange that)
  {
    NumericalRange lower, higher;
    if(this.compareTo(that) <= 0)
    {
      lower = this; higher = that;
    }
    else
    {
      lower = that; higher = this;
    }

    if(lower.getUpperClosed())
    {
      if(higher.getLowerClosed())
      {
        return lower.getUpper() >= higher.getLower();
      }
      else
      {
        return lower.getUpper() > higher.getLower();
      }
    }
    else
    {
      if(higher.getLowerClosed())
      {
        return lower.getUpper() > higher.getLower();
      }
      else
      {
        return lower.getUpper() >= higher.getLower();
      }
    }
  }

  // (*,2) indicates -infinity to 2 and [5,*) indicates 5 to infinity
  private static double handleRange(String range, boolean isLower)
  {
    Pattern pattern = isLower ? LOWER_PATTERN : UPPER_PATTERN;
    Matcher matcher = pattern.matcher(range);
    if (matcher.find())
    {
      String numberOrStarString = matcher.group(1);
      if("*".equals(numberOrStarString))
      {
        return isLower ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
      }
      else
      {
        return Double.parseDouble(numberOrStarString);
      }
    }
    else
    {
      throw new IllegalArgumentException("Can not parse bucket from range string " + range);
    }
  }

  @Override
  public int compareTo(NumericalRange o)
  {
    // Rule of the comparison:
    // 1. compare lower bound, close < open if they are equal
    // 2. compare upper bound, close > open if they are equal
    if (lower < o.lower)
    {
      return -1;
    }
    else if (lower == o.lower)
    {
      if (lowerClosed && !o.lowerClosed)
      {
        return -1;
      }
      else if (!lowerClosed && o.lowerClosed)
      {
        return 1;
      }
      else
      {
        if (upper < o.upper)
        {
          return -1;
        }
        else if (upper == o.upper)
        {
          if (!upperClosed && o.upperClosed)
            return -1;
          else if (upperClosed && !o.upperClosed)
            return 1;
          else
            return 0;
        }
        else
        {
          return 1;
        }
      }
    }
    else
    {
      return 1;
    }
  }
}
