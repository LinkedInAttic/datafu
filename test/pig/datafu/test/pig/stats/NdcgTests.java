package datafu.test.pig.stats;


import datafu.pig.stats.PositionScoringFunction;
import datafu.test.pig.PigTests;
import java.util.List;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * @author jhartman
 */
public class NdcgTests extends PigTests
{

  /**
   register $JAR_PATH

   define Ndcg datafu.pig.stats.Ndcg($POSITIONSCORES);

   data_in = LOAD 'input' as (val:double);

   --describe data_in;

   data_out = GROUP data_in ALL;

   --describe data_out;

   data_out = FOREACH data_out GENERATE Ndcg($1) AS ndcg;
   data_out = FOREACH data_out GENERATE FLATTEN(ndcg);

   --describe data_out;

   STORE data_out into 'output';
   */
  @Multiline
  private String ndcgTest;

  @Test
  public void testPositionalNdcg() throws Exception
  {
    PigTest test = createPigTestFromString(ndcgTest,
                                           "POSITIONSCORES='0:1.0','1:0.75','[2,3]:0.5','[4,5]:0.25','(5,*]:0.0'");

    String[] input = {"1.0", "0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1"};
    writeLinesToFile("input", input);

    test.runScript();

    List<Tuple> output = getLinesForAlias(test, "data_out", true);

    double expectedNumerator = 1*1 + .9*.75 + .8*.5 + .7*.5 + .6*.25+.5*.25;
    double expectedDenominator = 1 + .75 + 2*.5 + 2*.25;
    double expectedNdcg = expectedNumerator / expectedDenominator;

    assertEquals(output.size(),1);
    Assert.assertEquals(Double.parseDouble(output.get(0).get(0).toString()), expectedNdcg, 1e-9);
  }

  @Test
  public void testLogarithmicNdcg() throws Exception
  {
    PigTest test = createPigTestFromString(ndcgTest,
                                           "POSITIONSCORES='log2'");

    String[] input = {"1.0", "0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1"};
    writeLinesToFile("input", input);

    test.runScript();

    List<Tuple> output = getLinesForAlias(test, "data_out", true);

    double[] positionValues = {1, 1/log2(3), 1/log2(4), 1/log2(5), 1/log2(6), 1/log2(7), 1/log2(8), 1/log2(9), 1/log2(10), 1/log2(11)};
    double[] scores = {1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1};
    double numerator = dotProduct(positionValues, scores);
    double denominator = sum(positionValues);
    double expectedNdcg = numerator / denominator;

    assertEquals(output.size(),1);
    System.out.println(output);
    Assert.assertEquals(Double.parseDouble(output.get(0).get(0).toString()), expectedNdcg, 1e-9);
  }

  @Test
  public void testCustomDiscounter() throws Exception
  {
    PigTest test = createPigTestFromString(ndcgTest,
                                           "POSITIONSCORES='custom','datafu.pig.stats.UnaryScoringFunction'");

    String[] input = {"1.0", "0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1"};
    writeLinesToFile("input", input);

    test.runScript();

    List<Tuple> output = getLinesForAlias(test, "data_out", true);

    double[] scores = {1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1};
    double numerator = sum(scores);
    double denominator = 10;
    double expectedNdcg = numerator / denominator;

    assertEquals(output.size(),1);
    System.out.println(output);
    Assert.assertEquals(Double.parseDouble(output.get(0).get(0).toString()), expectedNdcg, 1e-9);
  }


  private final double sum(double... nums)
  {
    double sum = 0;
    for(double num : nums) sum += num;
    return sum;
  }

  private final double dotProduct(double[] a, double[] b)
  {
    if(a.length != b.length)
    {
      throw new IllegalArgumentException();
    }
    double sum = 0;
    for(int i = 0; i < a.length; i++)
    {
      sum += a[i] * b[i];
    }
    return sum;
  }

  private final double log2(int num)
  {
    return Math.log(num) / Math.log(2);
  }
}
