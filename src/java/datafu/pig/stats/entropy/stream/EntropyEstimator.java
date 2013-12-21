package datafu.pig.stats.entropy.stream;

import org.apache.pig.backend.executionengine.ExecException;


/*
* Entropy estimator which exposes a unified interface to application
* and hides the implementation details in its subclasses
*/
public abstract class EntropyEstimator {

    public static final String EMPIRICAL_ESTIMATOR = "empirical";
    
    public static final String CHAOSHEN_ESTIMATOR = "chaosh";
    
    static final String EULER = "e";
    
    /*
     * logarithm base
     * by default, we use Euler's number as the default logarithm base
     */ 
    protected String base;
    
    protected EntropyEstimator(String base) {
        this.base = base;
    }
    
    /* 
     * create estimator instance based on the input type
     * @param type  type of entropy estimator
     * @param base logarithm base
     * if the method is not supported, a null estimator instance is returned
     * and the external application needs to handle this case   
     */
    public static EntropyEstimator createEstimator(String type, String base) {
        //empirical estimator
        if(EmpiricalEntropyEstimator.EMPIRICAL_ESTIMATOR.equalsIgnoreCase(type)) {
            return new EmpiricalEntropyEstimator(base);
        }
        //chao-shen estimator
        if(EmpiricalEntropyEstimator.CHAOSHEN_ESTIMATOR.equalsIgnoreCase(type)) {
           return new ChaoShenEntropyEstimator(base);
        }
        //unsupported estimator type
        return null;
    }
    
    /*
     * accumulate occurrence frequency from input stream of samples
     * @param cx the occurrence frequency of the last input sample
     */
    public abstract void accumulate(long cx) throws ExecException;

    /*
     * calculate the output entropy value
     * return entropy value as a double type number
     */
    public abstract double getEntropy();

    /*
     * cleanup and reset internal states
     */
    public abstract void reset();

    /*
     * Transform the input entropy to that in the input logarithm base
     * The input entropy is assumed to be calculated in the Euler's number logarithm base
     * */
    protected double logTransform(double h) {
        if(this.base != null && 
           !this.base.isEmpty() &&
           !EULER.equalsIgnoreCase(this.base)) {
            try {
                
                double base = Double.parseDouble(this.base);
                
                if(base > 0) {
                    //if the input base is a positive number
                    h = h / Math.log(base);
                }
                
            } catch (NumberFormatException ex) {
                //invalid input logarithm base, use euler number as the default logarithm base
            }
        }
        
        return h;
    }
}
