package datafu.test.pig.text;

import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class LevenshteinDistanceTest extends PigTests
{
  
  @Test
  public void LevenshteinDistanceTest() throws Exception
  {
    // Note that this test runs with a maxDistance of 10.
    // A maxDistance value is recommended to speed up performance of the algorithm.
    PigTest test = createPigTest("test/pig/datafu/test/pig/text/levenshteinDistanceTest.pig");
  
    String[] input = {
        "kitten\tsitting",
        "dog\tcat",
        "alligator\tcrocodile",
        "dingo\tdino",
        "four hundred dogs\tfour hundred cats",
        "Lorem ipsum dolor sit amet, consectetur adipisicing elit\tlabore et dolore magna aliqua. Ut enim ad minim veniam"  //To test maxDistance parameter
    };
    
    String[] output = {
        "(3)",
        "(3)",
        "(9)",
        "(1)",
        "(3)",
        "(10)"
      };
    
    test.assertOutput("data",input,"data_out",output);
  }

}
