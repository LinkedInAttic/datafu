# 0.1.3

Fixes:

* Fix bug reusing output in execution planner.  It now subtracts off the correct old inputs.
* When reusing output, execution planner now considers whether it is better than not reusing.

# 0.1.2

Fixes:

* Correctly load schema for input paths.

# 0.1.1

Additions:

* Jobs now support joins on inconsistent schemas (Issue #67)

# 0.1.0

Initial release.
