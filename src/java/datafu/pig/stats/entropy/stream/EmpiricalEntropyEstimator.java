package datafu.pig.stats.entropy.stream;


/*
 * This entropy estimator calculates the empirical entropy of input samples
 * using per sample's frequency as an estimation of its probability. 
 * <p>
 * It applies the following formula to compute entropy:
 * H(X) = log(N) - 1 / N * SUM(c(x) * log(c(x)) )
 * c(x) is the occurrence frequency of sample x, N is the sum of c(x)
 * </p>
 */
class EmpiricalEntropyEstimator extends EntropyEstimator {
    
    //sum of frequency cx of all input samples
    private long N;
    
    //sum of cx * log(cx) of all input samples
    private double M;
    
    EmpiricalEntropyEstimator(String base){
        super(base);
        reset();
    }

    @Override
    public void accumulate(long cx) {
        N += cx;
        M += (cx == 0 ? 0 : cx * Math.log(cx));
    }

    @Override
    public double getEntropy() {
        return N > 0 ? super.logTransform(Math.log(N) - M / N) : 0;
    }
    
    @Override
    public void reset(){
        N = 0;
        M = 0;
    }
}
