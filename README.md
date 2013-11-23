# DataFu [![Build Status](https://travis-ci.org/linkedin/datafu.png?branch=master)](https://travis-ci.org/linkedin/datafu)

[DataFu](http://data.linkedin.com/opensource/datafu) is a collection of user-defined functions for working with large-scale data in Hadoop and Pig. This library was born out of the need for a stable, well-tested library of UDFs for data mining and statistics. It is used at LinkedIn in many of our off-line workflows for data derived products like "People You May Know" and "Skills & Endorsements". It contains functions for:

* PageRank
* Statistics (e.g. quantiles, median, variance, etc.)
* Sampling (e.g. weighted, reservoir, etc.)
* Sessionization
* Convenience bag functions (e.g. enumerating items)
* Convenience utility functions (e.g. assertions, easier writing of EvalFuncs)
* Set operations (intersect, union)
* and [more](http://linkedin.github.com/datafu/docs/current/)...

Each function is unit tested and code coverage is being tracked for the entire library.

We have also contributed a framework called [Hourglass](https://github.com/linkedin/datafu/tree/master/contrib/hourglass) for incrementally
processing data in Hadoop.

## Pig Compatibility

The current version of DataFu has been tested against Pig 0.11.1 and 0.12.0.  DataFu should be compatible with some older versions of Pig, however
we do not do any sort of testing with prior versions of Pig and do not guarantee compatibility.  
Our policy is to test against the most recent version of Pig whenever we release and make sure DataFu works with that version. 

## Blog Posts

* [Introducing DataFu](http://engineering.linkedin.com/open-source/introducing-datafu-open-source-collection-useful-apache-pig-udfs)
* [DataFu: The WD-40 of Big Data](http://hortonworks.com/blog/datafu/)
* [DataFu 1.0](http://engineering.linkedin.com/datafu/datafu-10)
* [DataFu's Hourglass: Incremental Data Processing in Hadoop](http://engineering.linkedin.com/datafu/datafus-hourglass-incremental-data-processing-hadoop)

## Presentations

* [A Brief Tour of DataFu](http://www.slideshare.net/matthewterencehayes/datafu)
* [Building Data Products at LinkedIn with DataFu](http://www.slideshare.net/matthewterencehayes/building-data-products-at-linkedin-with-datafu)
* [Hourglass: a Library for Incremental Processing on Hadoop (IEEE BigData 2013)](http://www.slideshare.net/matthewterencehayes/hourglass-a-library-for-incremental-processing-on-hadoop)

## Papers

* [Hourglass: a Library for Incremental Processing on Hadoop (IEEE BigData 2013)](http://www.slideshare.net/matthewterencehayes/hourglass-27038297)

## What can you do with it?

Here's a taste of what you can do in Pig.

### Statistics
  
Compute the [median](http://en.wikipedia.org/wiki/Median) with the [Median UDF](http://linkedin.github.com/datafu/docs/current/datafu/pig/stats/Median.html):

    define Median datafu.pig.stats.StreamingMedian();

    -- input: 3,5,4,1,2
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces median of 3
    medians = FOREACH grouped GENERATE Median(sorted.val);
  
Similarly, compute any arbitrary [quantiles](http://en.wikipedia.org/wiki/Quantile) with [StreamingQuantile](http://linkedin.github.com/datafu/docs/current/datafu/pig/stats/StreamingQuantile.html):

    define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.5','1.0');

    -- input: 9,10,2,3,5,8,1,4,6,7
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces: (1,5.5,10)
    quantiles = FOREACH grouped GENERATE Quantile(sorted.val);

Or how about the [variance](http://en.wikipedia.org/wiki/Variance) using [VAR](http://linkedin.github.com/datafu/docs/current/datafu/pig/stats/VAR.html):

    define VAR datafu.pig.stats.VAR();

    -- input: 1,2,3,4,5,6,7,8,9
    input = LOAD 'input' AS (val:int);

    grouped = GROUP input ALL;
    -- produces variance of 6.666666666666668
    variance = FOREACH grouped GENERATE VAR(input.val);
 
### Set Operations

Treat sorted bags as sets and compute their intersection with [SetIntersect](http://linkedin.github.com/datafu/docs/current/datafu/pig/sets/SetIntersect.html):

    define SetIntersect datafu.pig.sets.SetIntersect();
  
    -- ({(3),(4),(1),(2),(7),(5),(6)},{(0),(5),(10),(1),(4)})
    input = LOAD 'input' AS (B1:bag{T:tuple(val:int)},B2:bag{T:tuple(val:int)});

    -- ({(1),(4),(5)})
    intersected = FOREACH input {
      sorted_b1 = ORDER B1 by val;
      sorted_b2 = ORDER B2 by val;
      GENERATE SetIntersect(sorted_b1,sorted_b2);
    }
      
Compute the set union with [SetUnion](http://linkedin.github.com/datafu/docs/current/datafu/pig/sets/SetUnion.html):

    define SetUnion datafu.pig.sets.SetUnion();

    -- ({(3),(4),(1),(2),(7),(5),(6)},{(0),(5),(10),(1),(4)})
    input = LOAD 'input' AS (B1:bag{T:tuple(val:int)},B2:bag{T:tuple(val:int)});

    -- ({(3),(4),(1),(2),(7),(5),(6),(0),(10)})
    unioned = FOREACH input GENERATE SetUnion(B1,B2);
      
Operate on several bags even:

    intersected = FOREACH input GENERATE SetUnion(B1,B2,B3);

### Bag operations

Concatenate two or more bags with [BagConcat](http://linkedin.github.com/datafu/docs/current/datafu/pig/bags/BagConcat.html):

    define BagConcat datafu.pig.bags.BagConcat();

    -- ({(1),(2),(3)},{(4),(5)},{(6),(7)})
    input = LOAD 'input' AS (B1: bag{T: tuple(v:INT)}, B2: bag{T: tuple(v:INT)}, B3: bag{T: tuple(v:INT)});

    -- ({(1),(2),(3),(4),(5),(6),(7)})
    output = FOREACH input GENERATE BagConcat(B1,B2,B3);

Append a tuple to a bag with [AppendToBag](http://linkedin.github.com/datafu/docs/current/datafu/pig/bags/AppendToBag.html):

    define AppendToBag datafu.pig.bags.AppendToBag();

    -- ({(1),(2),(3)},(4))
    input = LOAD 'input' AS (B: bag{T: tuple(v:INT)}, T: tuple(v:INT));

    -- ({(1),(2),(3),(4)})
    output = FOREACH input GENERATE AppendToBag(B,T);

### PageRank

Run PageRank on a large number of independent graphs through the [PageRank UDF](http://linkedin.github.com/datafu/docs/current/datafu/pig/linkanalysis/PageRank.html):

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

    <dependency org="com.linkedin.datafu" name="datafu" rev="1.0.0"/>
    
If you are using Maven:

    <dependency>
      <groupId>com.linkedin.datafu</groupId>
      <artifactId>datafu</artifactId>
      <version>1.0.0</version>
    </dependency>

Or [download](https://github.com/linkedin/datafu/archive/master.zip) the code.
    
## Working with the source code

Here are some common tasks when working with the source code.

### Eclipse

To generate eclipse files:

    ant eclipse

### Build the JAR

    ant jar
    
### Run all tests

    ant test

### Run specific tests

Override `testclasses.pattern`, which defaults to `**/*.class`.  For example, to run all tests defined in `QuantileTests`:

    ant test -Dtestclasses.pattern=**/QuantileTests.class

### Compute code coverage

    ant coverage

### Notes on eclipse

#### Adjusting heap size for TestNG plugin

You may run out of heap when executing tests in Eclipse.  To fix this adjust your heap settings for the TestNG plugin.  Go to Eclipse->Preferences.  Select TestNG->Run/Debug.  Add "-Xmx1G" to the JVM args.

### Releasing 

We use Sonatype to release artifacts.  Information on how this is set up can be found [here](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide).  Most of this has already been set up with the `build.xml` file.  You will however need a Sonatype account and must create a Maven `~/.m2/settings.xml` with your account information, as described [here](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-7a.1.POMandsettingsconfig).

Create the settings.xml from our template:

    mkdir ~/.m2
    cp settings.xml.template ~/.m2/settings.xml

Then edit `~/.m2/settings.xml` and add your user name and password.

We use `gpg` to sign the artifacts, so you'll need `gpg` set up as well. Information on generating PGP signatures with `gpg` can be found [here](https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven).  Make sure you follow the section *Deleting a Sub Key*.

First run the tests to make sure all is well:

    ant test

If it succeeds, build artifacts and upload to sonatype:

    ant deploy

Login to [Sonatype](https://oss.sonatype.org/index.html#stagingRepositories).  In Staging Repositories you should see a repository for the articacts just uploaded.  Select the repository and click Close.

Now that the repository is closed you can download the artifacts and do some manual testing if you would like before doing a final release.   If you find a problem you can find drop the release by selecting the repository and clicking Drop.  Else to release select the repository and click Release.

## Contribute

The source code is available under the Apache 2.0 license.  

For help please see the [discussion group](http://groups.google.com/group/datafu).  Bugs and feature requests can be filed [here](http://github.com/linkedin/datafu/issues).
