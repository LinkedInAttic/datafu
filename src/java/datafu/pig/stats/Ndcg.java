package datafu.pig.stats;


import datafu.pig.util.SimpleEvalFunc;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Calculates a Normalized Discounted Cumulative Gain on a list of items.
 *
 * Datafu supports several different discounting algorithms out of the box.
 *
 * First is the usual log2 discounting function for a given position - log2(1 + pos) assuming 1-based indexing. Invoke Ndcg with the 'log2'
 * constructor first this behavior
 *
 * The last discounting algorithm supported by datafu is a range & index-based position discounting function.
 * Simply invoke the constructor with a list of numerical ranges for this behavior. See NumericalRange and RangeScoringFunction
 * for a detailed specification. The parameter is intended to honor the principle of least surprise.
 *
 * The following example should be pretty straightforward:
 *
 * DEFINE NDCG datafu.pig.stats.Ndcg('0: 1.0', '1: 0.8', '[2,4): 0.75', '[4,8): 0.6', '[8,*): 0.5')
 *
 * If you are building your own carefully tuned discounting function code, it is possible to plug in any PositionScoringFunction
 * into the Ndcg UDF. This can be done by invoking Ndcg such as:
 *
 * DEFINE NDCG datafu.pig.stats.Ndcg('custom', 'fully.qualified.class.name', 'arg1', 'arg2', ..., 'argn');
 *
 * Your constructor should take a list of strings and must implement the PositionScoringFunction interface

 */
public class Ndcg extends SimpleEvalFunc<Tuple>
{
  private final PositionScoringFunction positionDiscountingFunction;

  public Ndcg(String... config)
  {
    positionDiscountingFunction = fromConfig(config);
  }

  public static PositionScoringFunction fromConfig(String... config)
  {
    if(config.length == 1 && "log2".equals(config[0].trim()))
    {
      return new LogScoringFunction();
    }
    else if(config.length >=2 && "custom".equals(config[0].trim()))
    {
      String scoringFunctionClassName = config[1].trim();
      try
      {
        Class<?> specifiedClass = Class.forName(scoringFunctionClassName);
        try
        {
          Constructor<?> constructor = specifiedClass.getConstructor(String[].class);
          String[] parameters = Arrays.asList(config).subList(2, config.length).toArray(new String[config.length - 2]);
          try
          {
            return (PositionScoringFunction) constructor.newInstance((Object)parameters);
          }
          catch (Exception e)
          {
            throw new IllegalArgumentException("Could not instantiate the position scoring function", e);
          }
        }
        catch (NoSuchMethodException e)
        {
          throw new IllegalArgumentException("The constructor for class " + scoringFunctionClassName + "must implement a String[] constructor", e);
        }
      }
      catch (ClassNotFoundException e)
      {
        throw new IllegalArgumentException("Class " + scoringFunctionClassName + "could not be found on classpath", e);
      }
    }
    else
    {
      return new RangeScoringFunction(config);
    }
  }

  public Tuple call(DataBag bag) throws IOException
  {
    if (bag == null || bag.size() == 0)
      return null;

    double cumulativeGain = 0.0;
    double maxCumulativeGain = 0.0;
    int position = 0;

    for(Tuple t : bag)
    {
      Object o = t.get(0);
      if (!(o instanceof Number))
      {
        throw new IllegalArgumentException("bag must have numerical values (and be non-null)");
      }

      Number n = (Number) o;
      double itemScore = n.doubleValue();

      if(itemScore < 0 || itemScore > 1)
      {
        throw new IllegalArgumentException("Scores must be already normalized from 0 to 1");
      }

      double positionScore = positionDiscountingFunction.score(position);

      cumulativeGain += itemScore * positionScore;
      maxCumulativeGain += positionScore;

      position++;
    }

    double ndcg = cumulativeGain / maxCumulativeGain;

    Tuple t = TupleFactory.getInstance().newTuple(1);
    t.set(0, ndcg);
    return t;
  }

  @Override
  public Schema outputSchema(Schema inputSchema)
  {
    Schema tupleSchema = new Schema();
    tupleSchema.add(new Schema.FieldSchema("ndcg", DataType.DOUBLE));

    return tupleSchema;
  }
}
