# DataFu: Hourglass

Hourglass is a framework for incrementally processing partitioned data sets in Hadoop.  

## Quick Start Example

Let's walk through a use case where Hourglass is helpful.  Suppose that we have a website that tracks a particular event,
and for each event a member ID is recorded.  These events are collected and stored in HDFS in Avro under paths having the
format /data/event/yyyy/MM/dd.  Suppose for this example our Avro schema is:

```json
{
  "type" : "record",
  "name" : "ExampleEvent",
  "namespace" : "datafu.hourglass.test",
  "fields" : [ {
    "name" : "id",
    "type" : "long",
    "doc" : "ID"
  } ]
}
```

Suppose that the goal is to count how many times this event has occurred per member over the entire history and produce
a daily report summarizing these counts.  One solution is to simply consume all data under /data/event each day and 
aggregate by member ID.  However, most of the data is the same day to day.  The only difference is that each day a new
day of data appears in HDFS.  So while this solution works, it is wasteful.  Wouldn't it be better if we could merge
the previous result with the new data?  With Hourglass you can.

To continue our example, let's say there are two days of data currently available, 2013/03/15 and 2013/03/16, and that
their contents are:

```
2013/03/15:
{"id": 1}
{"id": 1}
{"id": 1}
{"id": 2}
{"id": 3}
{"id": 3}

2013/03/16:
{"id": 1}
{"id": 1}
{"id": 2}
{"id": 2}
{"id": 3}
```

Let's aggregate the counts by member ID using Hourglass.  To perform the aggregation we will use `PartitionCollapsingIncrementalJob`,
which essentially takes a partitioned data set like the one we have and collapses all the partitions together into a single output.

```Java
PartitionCollapsingIncrementalJob job = new PartitionCollapsingIncrementalJob(Example.class);
```

Next we will define the schemas for the key and value used by the job.  The key affects how data is grouped in the reducer when
we perform the aggregation.  In this case it will be the member ID.  The value is the piece of data being aggregated, which will
be an integer representing the count in this case.  Hourglass uses Avro for its data types.  Let's define the schemas:

```Java
final String namespace = "com.example";

final Schema keySchema = Schema.createRecord("Key",null,namespace,false);
keySchema.setFields(Arrays.asList(new Field("member_id",Schema.create(Type.LONG),null,null)));
final String keySchemaString = keySchema.toString(true);

final Schema valueSchema = Schema.createRecord("Value",null,namespace,false);
valueSchema.setFields(Arrays.asList(new Field("count",Schema.create(Type.INT),null,null)));
final String valueSchemaString = valueSchema.toString(true);
```

This produces schemas having the following representation:

```json
{
  "type" : "record",
  "name" : "Key",
  "namespace" : "com.example",
  "fields" : [ {
    "name" : "member_id",
    "type" : "long"
  } ]
}

{
  "type" : "record",
  "name" : "Value",
  "namespace" : "com.example",
  "fields" : [ {
    "name" : "count",
    "type" : "int"
  } ]
}
```

Now we can tell the job what our schemas are.  Hourglass allows two different value types.  One is the intermediate value type
that is output by the mapper and combiner.  The other is the output value type, the output of the reducer.  In this case we
will use the same value type for each.

```Java
job.setKeySchema(keySchema);
job.setIntermediateValueSchema(valueSchema);
job.setOutputValueSchema(valueSchema);
```

Next we will tell Hourglass where to find the data, where to write the data, and that we want to reuse the previous output.

```Java
job.setInputPaths(Arrays.asList(new Path("/data/event")));
job.setOutputPath(new Path("/output"));
job.setReusePreviousOutput(true);
```

Now let's get into some application logic.  The mapper will produce a key-value pair from each record consisting of
the member ID and a count, which for each input record will just be `1`.

```Java
job.setMapper(new Mapper<GenericRecord,GenericRecord,GenericRecord>() 
{
  private transient Schema kSchema;
  private transient Schema vSchema;
  
  @Override
  public void map(GenericRecord input,
                  KeyValueCollector<GenericRecord, GenericRecord> collector) throws IOException,
      InterruptedException
  {
    if (kSchema == null) kSchema = new Schema.Parser().parse(keySchemaString);
    if (vSchema == null) vSchema = new Schema.Parser().parse(valueSchemaString);
    GenericRecord key = new GenericData.Record(kSchema);
    key.put("member_id", input.get("id"));
    GenericRecord value = new GenericData.Record(vSchema);
    value.put("count", 1);
    collector.collect(key,value);
  }      
});
```

An accumulator is responsible for aggregating this data.  Records will be grouped by member and then passed to the accumulator
one-by-one.  The accumulator keeps a running count and adds each input count to it.  When all data has been passed to it
the `getFinal()` method will be called, which returns the output record containing the count.

```Java
job.setReducerAccumulator(new Accumulator<GenericRecord,GenericRecord>() 
{
  private transient int count;
  private transient Schema vSchema;
  
  @Override
  public void accumulate(GenericRecord value)
  {
    this.count += (Integer)value.get("count");
  }

  @Override
  public GenericRecord getFinal()
  {
    if (vSchema == null) vSchema = new Schema.Parser().parse(valueSchemaString);
    GenericRecord output = new GenericData.Record(vSchema);
    output.put("count", count);
    return output;
  }

  @Override
  public void cleanup()
  {
    this.count = 0;
  }      
});
```

Since the intermediate and output values have the same schema, the accumulator can also be used for the combiner,
so let's indicate that we want it to be used for that:

```Java
job.setCombinerAccumulator(job.getReducerAccumulator());
job.setUseCombiner(true);
```

Finally, we run the job.

```Java
job.run();
```

When we inspect the output we find that the counts match what we expect:

```
{"key": {"member_id": 1}, "value": {"count": 5}}
{"key": {"member_id": 2}, "value": {"count": 3}}
{"key": {"member_id": 3}, "value": {"count": 3}}
```

Now suppose that a new day of data becomes available:

```
2013/03/17:
{"id": 1}
{"id": 1}
{"id": 2}
{"id": 2}
{"id": 2}
{"id": 3}
{"id": 3}
```

Let's run the job again.
Since Hourglass already has a result for the previous day, it consumes the new day of input and the previous output, rather
than all the input data it already processed.  The previous output is passed to the accumulator implementation where it is
aggregated with the new data.  This produces the output we expect:

```json
{"key": {"member_id": 1}, "value": {"count": 7}}
{"key": {"member_id": 2}, "value": {"count": 6}}
{"key": {"member_id": 3}, "value": {"count": 5}}
```

In this example we only have a few days of input data, so the impact of incrementally processing the new data is small.
However, as the size of the input data grows, the benefit of incrementally processing data becomes very significant.

## Motivation

Data sets that are temporal in nature very often are stored in such a way that each directory corresponds to a separate
time range.  For example, one convention could be to divide the data by day.  One benefit of a partitioning scheme such 
as this is that it makes it possible to consume a subset of the data for specific time ranges, instead of consuming
the entire data set.

Very often computations on data such as this are performed daily over sliding windows.  For example, a metric of interest
may be the last time each member logged into the site.  The most straightforward implementation is to consume all login events
across all days.  This is inefficient however since day-to-day the input data is mostly the same.  
A more efficient solution is to merge the previous output with new data since the last run.  As a result there is less 
data to process.  Another metric of interest may be the number of pages viewed per member over the last 30 days.  
A straightforward implementation is to consume the page view data over the last 30 days each time the job runs.
However, again, the input data is mostly the same day-to-day.
Instead, given the previous output of the job, the new output can be produced by adding the new data and subtracting the old data.

Although these incremental jobs are conceptually easy to understand, the implementations can be complex.  
Hourglass defines an easy-to-use programming model and provides jobs for incrementally processing partitioned data as just described.
It handles the underlying details and complexity of an incremental system so that programmers can focus on
application logic.

## Capabilities

Hourglass uses Avro for input, intermediate, and output data.  Input data must be partitioned by day according to the
naming convention yyyy/MM/dd.  Joining multiple inputs is supported.

Hourglass provides two types of jobs: partition-preserving and partition-collapsing.  A *partition-preserving* job 
consumes input data partitioned by day and produces output data partitioned by day. This is equivalent to running a 
MapReduce job for each individual day of input data, but much more efficient.  It compares the input data against 
the existing output data and only processes input data with no corresponding output. A *partition-collapsing* job 
consumes input data partitioned by day and produces a single output.  What distinguishes this job from a standard 
MapReduce job is that it can reuse the previous output.  This enables it to process data much more efficiently.  
Rather than consuming all input data on each run, it can consume only the new data since the previous run and 
merges it with the previous output.  Since the partition-preserving job output partitioned data, the two jobs
can be chained together.

Given these two jobs, processing data over sliding windows can be done much more efficiently.  There are two types
of sliding windows that are of particular interest that can be implemented using Hourglass.  
A *fixed-start* sliding window has a start date that remains the same over multiple runs and an end date that is 
flexible, where the end date advances forward as new input data becomes available.
A *fixed-length* sliding window has a window length that remains the same over multiple runs and flexible start
and end dates, where the start and end advance forward as new input data becomes available.
Hourglass makes defining these sliding windows easy.

An example of a fixed-start sliding window problem is computing the last login time for all members of a website
using a login event that records the login time.  This could be solved efficiently by using a partition-collapsing
job, which is capable of reusing the previous output and merging it with new login data as it arrives.

An example of a fixed-length sliding window problem is computing the pages viewed per member of the last 30 days
using a page-view event.  This could also be solved efficiently using a partition-collapsing job, which is
capable of reusing the previous output and merging it with the new page-view data while subtracting off the old
page-view data.

For some problems it is not possible to subtract off the oldest day of data for a fixed-length sliding window problem.
For example, suppose the goal is to estimate the distinct number of members who have logged into a website in the last
30 days using a login event that records the member ID.  A HyperLogLog counter could be used to estimate the cardinality.
The internal data for this counter could be serialized to bytes and stored as output alongside the estimated count.
However, although multiple HyperLogLog counters can be merged together, they cannot be subtracted or unmerged.
In other words the operation is not reversible.  So a partition-collapsing job by itself could not be used.
However we could chain together a partition-preserving and partition-collapsing job.  The first job would estimate cardinality
per day and store the value with the counter's byte representation.  The second job would merge the per day counters
together to produce the estimate over the full 30 day window.  This makes the computation extremely efficient.

## Programming Model

To implement an incremental job, a developer must specify Avro schemas for the key, intermediate value, and output value types.
The key and intermediate value types are used for the output of the mapper and an optional combiner.  The key and output value
types are used for the output of the reducer.  The input schemas are automatically determined by the job by inspecting the
input data.

A developer must also define their application logic through interfaces that are based on the MapReduce programming model.
The mapper is defined through a *Mapper* interface, which given a record produces zero or more key-value pairs.
The key-value pairs must conform to the key and intermediate value schemas just mentioned.

Reduce is defined through an *Accumulator* interface, which is passed all the records for a given key and then returns
the final result.  Both the combiner and reducer use an Accumulator for the reduce functionality.
This is similar to the standard reduce function of MapReduce.  The key difference is that
no more than one record can be returned.  The input records to the Accumulator are of the intermediate value type; 
the ouput is of the output value type.  

If the intermediate and output value types are the same then the Accumulator can
naturally be used to merge the new input data with the previous output.  However if they are different then a class implementing
the *Merge* interface must be provided.  Merge is a a binary operation on two records of the output value type that returns a 
record as a result.

The other case where an implementation of Merge must be provided is when output is reused for a partition-collapsing job
over a fixed-length sliding window.  Merge in this case is used to essentially subtract old output from the current output.

## Contribute

The source code is available under the Apache 2.0 license.  Contributions are welcome.