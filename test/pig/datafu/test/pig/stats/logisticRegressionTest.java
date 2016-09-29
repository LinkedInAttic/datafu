package datafu.test.pig.stats;

import static org.testng.Assert.*;

import java.util.List;

import org.apache.pig.data.Tuple;

import datafu.test.pig.PigTests;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;


public class logisticRegressionTest extends PigTests {

    @Test
    public void lrTest() throws Exception
    {
        PigTest test = createPigTest("test/pig/datafu/test/pig/stats/logisticRegressionTests.pig");

        writeLinesToFile("input",
                "0,0.5,0.3",
                "1,0.2,0.4",
                "0,0.1,0.8",
                "1,0.7,0.9",
                "1,0.3,0.1",
                "1,0.1,0.2",
                "0,0.2,0.9",
                "0,0.3,0.8",
                "1,0.9,0.7");

        test.runScript();

        List<Tuple> output = this.getLinesForAlias(test, "logreg");

        String expectedOutput = "1.14012,4.30519,-4.22922";
        // ((1.1401200074379403,4.305190963019559,-4.229220862510476))

        for (Tuple t : output) {

            Double coef = (Double)((Tuple)t.get(0)).get(0);
            Double firstWeight = (Double)((Tuple)t.get(0)).get(1);
            Double secondWeight = (Double)((Tuple)t.get(0)).get(2);

            System.out.println("output: "+coef+" "+firstWeight+" "+secondWeight);

            assertEquals(String.format("%.5f,%.5f,%.5f",coef,firstWeight,secondWeight),expectedOutput);
        }

    }
}
