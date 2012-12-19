package datafu.pig.bags;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Usage:
 *
 * register lib/logreg.jar;
 * DEFINE LogisticRegressionFunc datafu.pig.bags.LogisticRegressionFunc();
 * samp = load 'sample.csv' using PigStorage(',') as (label:int,feature1:double,feature2:double);
 * group_samp = group samp all;
 * logreg = foreach group_samp generate LogisticRegressionFunc(samp);
 * dump logreg;
 *
 * expected format of input data:
 *  tuple of a bag
 *  the bag contains tuples
 *  each tuple with
 *      first value: dependant/response variable
 *      rest of the value: independent variables (features)
 *
 * Returns a tuple with intercept and weights/coefficient for each independent variable (in the order given)
 *
 * @author Mitul Tiwari (mtiwari)
 * 
 */
public class LogisticRegressionFunc extends EvalFunc<Tuple> {

  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  int numEpochs;
  Double learningRate;
  ArrayList<Double> featureWeights;

  public LogisticRegressionFunc(){
    numEpochs = 10000;
    learningRate = 0.1d;
    featureWeights = new ArrayList<Double>();
  }

  public Tuple exec(Tuple input) throws IOException {

    DataBag inputBag = (DataBag) input.get(0);

    System.out.println("learningRate: "+learningRate);
    boolean initialized = false;

    for(int j = 0; j < numEpochs; j++){

      for(Tuple t : inputBag) {

        if(!initialized){
          for(int i = 0; i<t.size(); i++){
            featureWeights.add(0.0d);
          }
          initialized = true;
        }

        Double label = (double) ((Integer)t.get(0)).intValue();
        ArrayList<Double> featureValues = new ArrayList<Double>();
        featureValues.add(1.0d); // for intercept
        for(int i=1; i < t.size(); i++) {
          featureValues.add((Double)t.get(i));
        }
        trainOne(label, featureValues);
      }
    }

    // set return tuple
    Tuple weightsTuple = tupleFactory.newTuple(featureWeights.size());
    for(int i=0; i < featureWeights.size(); i++) {
      weightsTuple.set(i, featureWeights.get(i));
    }

    return weightsTuple;
  }

  // Inputs: lable, features: a list of feature values in double
  void trainOne(Double label, ArrayList<Double> featureValues){
    double prob = classify(featureValues);
    for(int i = 0; i<featureValues.size(); i++) {
      double weight = 0.0d;
      weight = featureWeights.get(i);
      double newweight = weight + learningRate * (label - prob) * featureValues.get(i);
      featureWeights.set(i, newweight);
    }
  }

  double classify(ArrayList<Double> featureValues){
    double prob = 0.0d;
    double z = 0.0d;
    for(int i=0; i < featureValues.size(); i++) {
      Double value = featureValues.get(i);
      double weight = 0.0d;
      weight = featureWeights.get(i);
      z = z + weight * value;
    }
    prob = 1/(1+Math.exp(-z));
    return prob;
  }

  @Override
    public Schema outputSchema(Schema input) {
      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.TUPLE));
    }
}
