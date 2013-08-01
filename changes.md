# 1.0.0

Additions:

* Created SampleByKey, which provides a way to sample tuples based on certain fields.
* Created Coalesce, which returns the first non-null value from a list of arguments.
* Sessionize now supports long values for timestamp, in addition to string representation of time.
* Added ReservoirSample
* BagConcat can now operate on a bag of bags, in addition to a tuple of bags
* Better documentation and more test cases across the board
* Created TransposeTupleToBag, which creates a bag of key-value pairs from a tuple
* SessionCount now implements Accumulator interface
* DistinctBy now implements Accumulator interface
* Using pigunit from Maven for testing

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