/**
 * Incremental Hadoop jobs and some supporting classes.  
 * 
 * <p>
 * Jobs within this package form the core of the incremental framework implementation.
 * There are two types of incremental jobs: <em>partition-preserving</em> and 
 * <em>partition-collapsing</em>.
 * </p>
 * 
 * <p>
 * A partition-preserving job consumes input data partitioned by day and produces output data partitioned by day.
 * This is equivalent to running a MapReduce job for each individual day of input data,
 * but much more efficient.  It compares the input data against the existing output data and only processes
 * input data with no corresponding output.
 * </p>
 * 
 * <p>
 * A partition-collapsing job consumes input data partitioned by day and produces a single output.
 * What distinguishes this job from a standard MapReduce job is that it can reuse the previous output.
 * This enables it to process data much more efficiently.  Rather than consuming all input data on each
 * run, it can consume only the new data since the previous run and merges it with the previous output.
 * </p>
 * 
 * <p>
 * Partition-preserving and partition-collapsing jobs can be created by extending {@link datafu.hourglass.jobs.AbstractPartitionPreservingIncrementalJob}
 * and {@link datafu.hourglass.jobs.AbstractPartitionCollapsingIncrementalJob}, respectively, and implementing the necessary methods.
 * Alternatively, there are concrete versions of these classes, {@link datafu.hourglass.jobs.PartitionPreservingIncrementalJob} and 
 * {@link datafu.hourglass.jobs.PartitionCollapsingIncrementalJob}, which can be used instead.  With these classes, the implementations are provided
 * through setters.  
 * </p>
 * 
 * <p>
 * Incremental jobs use Avro for input, intermediate, and output data.  To implement an incremental job, one must define their schemas.
 * A <em>key schema</em> and <em>intermediate value schema</em> specify the output of the mapper and combiner, which output key-value pairs.
 * The <em>key schema</em> and an <em>output value schema</em> specify the output of the reducer, which outputs a record having key and value
 * fields.
 * </p>
 * 
 * <p>
 * An incremental job also requires that implementations of map and reduce be defined, and optionally combine.  The map implementation must 
 * implement a {@link datafu.hourglass.model.Mapper} interface, which is very similar to the standard map interface in Hadoop.
 * The combine and reduce operations are implemented through an {@link datafu.hourglass.model.Accumulator} interface.
 * This is similar to the standard reduce in Hadoop, however values are provided one-at-a-time rather than by an enumerable list.
 * Also an accumulator returns either one value or no value at all by returning null.  That is, the accumulator may not return an arbitrary number of values
 * for the output.  This restriction precludes the implementation of certain operations, like flatten, which do not fit well within the 
 * incremental programming model.
 * </p>
 */
package datafu.hourglass.jobs;