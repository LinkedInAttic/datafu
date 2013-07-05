package datafu.test.pig.sampling;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;


public class SamplingTests extends PigTests
{
  /**
  register $JAR_PATH

  define WeightedSample datafu.pig.sampling.WeightedSample('1');
  
  data = LOAD 'input' AS (A: bag {T: tuple(v1:chararray,v2:INT)});
  
  data2 = FOREACH data GENERATE WeightedSample(A,1);
  --describe data2;
  
  STORE data2 INTO 'output';

   */
  @Multiline
  private String weightedSampleTest;
  
  @Test
  public void weightedSampleTest() throws Exception
  {
    PigTest test = createPigTestFromString(weightedSampleTest);

    writeLinesToFile("input", 
                     "({(a, 100),(b, 1),(c, 5),(d, 2)})");
                  
    test.runScript();
            
    assertOutput(test, "data2",
        "({(a,100),(c,5),(b,1),(d,2)})");
  }
  
  /**
  register $JAR_PATH

  define WeightedSample datafu.pig.sampling.WeightedSample('1');
  
  data = LOAD 'input' AS (A: bag {T: tuple(v1:chararray,v2:INT)});
  
  data2 = FOREACH data GENERATE WeightedSample(A,1,3);
  --describe data2;
  
  STORE data2 INTO 'output';
   */
  @Multiline
  private String weightedSampleLimitTest;
  
  @Test
  public void weightedSampleLimitTest() throws Exception
  {
    PigTest test = createPigTestFromString(weightedSampleLimitTest);

    writeLinesToFile("input", 
                     "({(a, 100),(b, 1),(c, 5),(d, 2)})");
                  
    test.runScript();
            
    assertOutput(test, "data2",
        "({(a,100),(c,5),(b,1)})");
  }
  
  /**
  register $JAR_PATH
  
  DEFINE SampleByKey datafu.pig.sampling.SampleByKey('salt2.5', '0.5');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FILTER data BY SampleByKey(A_id);
  
  STORE sampled INTO 'output';

   */
  @Multiline
  private String sampleByKeyTest;
  
  @Test
  public void sampleByKeyTest() throws Exception
  {
    PigTest test = createPigTestFromString(sampleByKeyTest);
    
    writeLinesToFile("input",
                     "A1\tB1\t1","A1\tB1\t4","A1\tB3\t4","A1\tB4\t4",
                     "A2\tB1\t4","A2\tB2\t4",
                     "A3\tB1\t3","A3\tB1\t1","A3\tB3\t77",
                     "A4\tB1\t3","A4\tB2\t3","A4\tB3\t59","A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3","A6\tB2\t55","A6\tB3\t1",
                     "A7\tB1\t39","A7\tB2\t27","A7\tB3\t85",
                     "A8\tB1\t4","A8\tB2\t45",
                     "A9\tB3\t92", "A9\tB1\t42","A9\tB2\t1","A9\tB3\t0",
                     "A10\tB1\t7","A10\tB2\t23","A10\tB3\t1","A10\tB4\t41","A10\tB5\t52");
    
    test.runScript();
    assertOutput(test, "sampled", 
                 "(A4,B1,3)","(A4,B2,3)","(A4,B3,59)","(A4,B4,29)",
                 "(A5,B1,4)",
                 "(A6,B2,3)","(A6,B2,55)","(A6,B3,1)",
                 "(A7,B1,39)","(A7,B2,27)","(A7,B3,85)",
                 "(A10,B1,7)","(A10,B2,23)","(A10,B3,1)","(A10,B4,41)","(A10,B5,52)");
  }

  /**
  register $JAR_PATH
  
  DEFINE SampleByKey datafu.pig.sampling.SampleByKey('salt2.5', '0.5');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FILTER data BY SampleByKey(A_id, B_id);
  
  STORE sampled INTO 'output';

   */
  @Multiline
  private String sampleByKeyMultipleKeyTest;
  
  @Test
  public void sampleByKeyMultipleKeyTest() throws Exception
  {
    PigTest test = createPigTestFromString(sampleByKeyMultipleKeyTest);
    
    writeLinesToFile("input",
                     "A1\tB1\t1","A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3","A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3","A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92","A9\tB3\t0",
                     "A9\tB6\t42","A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23","A10\tB2\t1","A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");
    test.runScript();
    assertOutput(test, "sampled", 
                 "(A1,B1,1)","(A1,B1,4)",
                 "(A1,B4,4)",
                 "(A2,B1,4)",
                 "(A2,B2,4)",
                 "(A3,B1,3)","(A3,B1,1)",
                 "(A4,B4,29)",
                 "(A5,B1,4)",
                 "(A6,B3,1)",
                 "(A7,B1,39)",
                 "(A8,B1,4)",
                 "(A9,B3,92)","(A9,B3,0)",
                 "(A10,B2,23)","(A10,B2,1)","(A10,B2,31)"
                 );
                   
  }
  
  /**
  register $JAR_PATH

  DEFINE ReservoirSample datafu.pig.sampling.ReservoirSample('$RESERVOIR_SIZE');
  
  data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
  sampled = FOREACH (GROUP data ALL) GENERATE ReservoirSample(data) as sample_data;
  sampled = FOREACH sampled GENERATE COUNT(sample_data) AS sample_count;
  STORE sampled INTO 'output';

   */
  @Multiline
  private String reservoirSampleTest;
  
  @Test
  public void reservoirSampleTest() throws Exception
  {
    
    writeLinesToFile("input",
                     "A1\tB1\t1",
                     "A1\tB1\t4",
                     "A1\tB3\t4",
                     "A1\tB4\t4",
                     "A2\tB1\t4",
                     "A2\tB2\t4",
                     "A3\tB1\t3",
                     "A3\tB1\t1",
                     "A3\tB3\t77",
                     "A4\tB1\t3",
                     "A4\tB2\t3",
                     "A4\tB3\t59",
                     "A4\tB4\t29",
                     "A5\tB1\t4",
                     "A6\tB2\t3",
                     "A6\tB2\t55",
                     "A6\tB3\t1",
                     "A7\tB1\t39",
                     "A7\tB2\t27",
                     "A7\tB3\t85",
                     "A8\tB1\t4",
                     "A8\tB2\t45",
                     "A9\tB3\t92",
                     "A9\tB3\t0",
                     "A9\tB6\t42",
                     "A9\tB5\t1",
                     "A10\tB1\t7",
                     "A10\tB2\t23",
                     "A10\tB2\t1",
                     "A10\tB2\t31",
                     "A10\tB6\t41",
                     "A10\tB7\t52");
   
    for(int i=10; i<=30; i=i+10){
      int reservoirSize = i ;
      PigTest test = createPigTestFromString(reservoirSampleTest, "RESERVOIR_SIZE="+reservoirSize);
      test.runScript();
      assertOutput(test, "sampled", "("+reservoirSize+")");
    }
  }
}
