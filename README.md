# DataFu

DataFu is a collection of user-defined functions for working with large-scale data in Hadoop and Pig. This library was born out of the need for a stable, well-tested library of UDFs for data mining and statistics. It is used at LinkedIn in many of our off-line workflows for data derived products like "People You May Know" and "Skills & Endorsements". It contains functions for:

* PageRank
* Quantiles (median), variance, etc.
* Sessionization
* Convenience bag functions (e.g., set operations, enumerating bags, etc)
* Convenience utility functions (e.g., assertions, easier writing of
EvalFuncs)
* and [more](http://linkedin.github.com/datafu/docs/javadoc/)...

Each function is unit tested and code coverage is being tracked for the entire library.  It has been tested against Pig 0.10.

[http://data.linkedin.com/opensource/datafu](http://data.linkedin.com/opensource/datafu)

## What can you do with it?

Here's a taste of what you can do in Pig.

### Statistics
  
Compute the [median](http://en.wikipedia.org/wiki/Median):

    define Median datafu.pig.stats.StreamingMedian();

    -- input: 3,5,4,1,2
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces median of 3
    medians = FOREACH grouped GENERATE Median(sorted.val);
  
Similarly, compute any arbitrary [quantiles](http://en.wikipedia.org/wiki/Quantile):

    define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.5','1.0');

    -- input: 9,10,2,3,5,8,1,4,6,7
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces: (1,5.5,10)
    quantiles = FOREACH grouped GENERATE Quantile(sorted.val);

Or how about the [variance](http://en.wikipedia.org/wiki/Variance):

    define VAR datafu.pig.stats.VAR();

    -- input: 1,2,3,4,5,6,7,8,9
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces variance of 7.5
    variance = FOREACH grouped GENERATE VAR(input.val);
 
### Set Operations

Treat sorted bags as sets and compute their intersection:

    define SetIntersect datafu.pig.bags.sets.SetIntersect();
  
    -- ({(3),(4),(1),(2),(7),(5),(6)},{(0),(5),(10),(1),(4)})
    input = LOAD 'input' AS (B1:bag{T:tuple(val:int)},B2:bag{T:tuple(val:int)});

    -- ({(1),(4),(5)})
    intersected = FOREACH input {
      sorted_b1 = ORDER B1 by val;
      sorted_b2 = ORDER B2 by val;
      GENERATE SetIntersect(sorted_b1,sorted_b2);
    }
      
Compute the set union:

    define SetUnion datafu.pig.bags.sets.SetUnion();

    -- ({(3),(4),(1),(2),(7),(5),(6)},{(0),(5),(10),(1),(4)})
    input = LOAD 'input' AS (B1:bag{T:tuple(val:int)},B2:bag{T:tuple(val:int)});

    -- ({(3),(4),(1),(2),(7),(5),(6),(0),(10)})
    unioned = FOREACH input GENERATE SetUnion(B1,B2);
      
Operate on several bags even:

    intersected = FOREACH input GENERATE SetUnion(B1,B2,B3);

### Bag operations

Concatenate two or more bags:

    define BagConcat datafu.pig.bags.BagConcat();

    -- ({(1),(2),(3)},{(4),(5)},{(6),(7)})
    input = LOAD 'input' AS (B1: bag{T: tuple(v:INT)}, B2: bag{T: tuple(v:INT)}, B3: bag{T: tuple(v:INT)});

    -- ({(1),(2),(3),(4),(5),(6),(7)})
    output = FOREACH input GENERATE BagConcat(B1,B2,B3);

Append a tuple to a bag:

    define AppendToBag datafu.pig.bags.AppendToBag();

    -- ({(1),(2),(3)},(4))
    input = LOAD 'input' AS (B: bag{T: tuple(v:INT)}, T: tuple(v:INT));

    -- ({(1),(2),(3),(4)})
    output = FOREACH input GENERATE AppendToBag(B,T);

### PageRank

Run PageRank on a large number of independent graphs:

    define PageRank datafu.pig.linkanalysis.PageRank('dangling_nodes','true');

    topic_edges = LOAD 'input_edges' as (topic:INT,source:INT,dest:INT,weight:DOUBLE);

    topic_edges_grouped = GROUP topic_edges by (topic, source) ;
    topic_edges_grouped = FOREACH topic_edges_grouped GENERATE
      group.topic as topic,
      group.source as source,
      topic_edges.(dest,weight) as edges;

    topic_edges_grouped_by_topic = GROUP topic_edges_grouped BY topic; 

    topic_ranks = FOREACH topic_edges_grouped_by_topic GENERATE
      group as topic,
      FLATTEN(PageRank(topic_edges_grouped.(source,edges))) as (source,rank);

    skill_ranks = FOREACH skill_ranks GENERATE
      topic, source, rank;
    
This implementation stores the nodes and edges (mostly) in memory. It is therefore best suited when one needs to compute PageRank on many reasonably sized graphs in parallel.
    
## Start Using It

The JAR can be found [here](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.linkedin.datafu%22) in the Maven central repository.  The GroupId and ArtifactId are `com.linkedin.datafu` and `datafu`, respectively.

If you are using Ivy:

    <dependency org="com.linkedin.datafu" name="datafu" rev="0.0.4"/>
    
If you are using Maven:

    <dependency>
      <groupId>com.linkedin.datafu</groupId>
      <artifactId>datafu</artifactId>
      <version>0.0.4</version>
    </dependency>
    
Or you can download one of the packages from the [downloads](https://github.com/linkedin/datafu/downloads) section.    

## Working with the source code

Here are some common tasks when working with the source code.

### Build the JAR

    ant jar
    
### Run all tests

    ant test

### Run specific tests

Override `testclasses.pattern`, which defaults to `**/*.class`.  For example, to run all tests defined in `QuantileTests`:

    ant test -Dtestclasses.pattern=**/QuantileTests.class

### Compute code coverage

    ant coverage

## Contribute

The source code is available under the Apache 2.0 license.  

For help please see the [discussion group](http://groups.google.com/group/datafu).  Bugs and feature requests can be filed [here](http://linkedin.jira.com/browse/DATAFU).
