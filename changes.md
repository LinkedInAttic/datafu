# 1.2.0

Additions:

* Pair of UDFs for simple random sampling with replacement.
* More dependencies now packaged in DataFu so fewer JAR dependencies required.
* SetDifference UDF for computing set difference A-B or A-B-C.
* HyperLogLogPlusPlus UDF for efficient cardinality estimation.

# 1.1.0

This release adds compatibility with Pig 0.12 (courtesy of jarcec).

Additions:

* Added SHA hash UDF.
* InUDF and AssertUDF added for Pig 0.12 compatibility.  These are the same as In and Assert.
* SimpleRandomSample, which implements a scalable simple random sampling algorithm.

Fixes:

* Fixed the schema declarations of several UDFs for compatibility with Pig 0.12, which is now stricter with schemas.

# 1.0.0

**This is not a backwards compatible release.**

Additions:

* Added SampleByKey, which provides a way to sample tuples based on certain fields.
* Added Coalesce, which returns the first non-null value from a list of arguments like SQL's COALESCE.
* Added BagGroup, which performs an in-memory group operation on a bag.
* Added ReservoirSample
* Added In filter func, which behaves like SQL's IN
* Added EmptyBagToNullFields, which enables multi-relation left joins using COGROUP
* Sessionize now supports long values for timestamp, in addition to string representation of time.
* BagConcat can now operate on a bag of bags, in addition to a tuple of bags
* Created TransposeTupleToBag, which creates a bag of key-value pairs from a tuple
* SessionCount now implements Accumulator interface
* DistinctBy now implements Accumulator interface
* Using PigUnit from Maven for testing, instead of checked-in JAR
* Added many more test cases to improve coverage
* Improved documentation

Changes:

* Moved WeightedSample to datafu.pig.sampling
* Using Pig 0.11.1 for testing.
* Renamed package datafu.pig.numbers to datafu.pig.random
* Renamed package datafu.pig.bag.sets to datafu.pig.sets
* Renamed TimeCount to SessionCount, moved to datafu.pig.sessions
* ASSERT renamed to Assert
* MD5Base64 merged into MD5 implementation, constructor arg picks which method, default being hex

Removals:

* Removed ApplyQuantiles
* Removed AliasBagFields, since can now achieve with nested foreach

Fixes:

* Quantile now outputs schemas consistent with StreamingQuantile
* Necessary fastutil classes now packaged in datafu JAR, so fastutil JAR not needed as dependency
* Non-deterministic UDFs now marked as so

# 0.0.10

Additions:

* CountEach now implements Accumulator
* Added AliasableEvalFunc, a base class to enable UDFs to access fields in tuple by name instead of position
* Added BagLeftOuterJoin, which can perform left join on two or more reasonably sized bags without a reduce

Fixes:

* StreamingQuantile schema fix

# 0.0.9

Additions:

* WeightedSample can now take a seed

Changes:

* Test against Pig 0.11.0

Fixes:

* Null pointer fix for Enumerate's Accumulator implementation