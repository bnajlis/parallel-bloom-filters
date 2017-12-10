# parallel-bloom-filters
Scalable Parallelized version of Bloom Filters
==============================================

A Bloom filter is a space-efficient probabilistic data structure, conceived by Burton Howard Bloom in 1970, that is used to test whether an element is a member of a set. False positive matches are possible, but false negatives are not – in other words, a query returns either "possibly in set" or "definitely not in set". Elements can be added to the set, but not removed (though this can be addressed with a "counting" filter); the more elements that are added to the set, the larger the probability of false positives. [Wikipedia: https://en.wikipedia.org/wiki/Bloom_filter]

This parallelized version of Bloom Filters uses Spark to scale the filter to massive data sets. The code was created using Python (Pyspark) and includes three scripts:

** generate-sample-data.py: Used to generate random sample data, used for benchmarking and testing purposes
** parallel-bloom-filter.py: Includes the code to generate the bloom filter structure, and test for elements existance in the set

The code is simple enough so that it can be extended to, for example, be used with Spark Streaming for testing purposes.

Details on Bloom Filters implementation
=======================================

Being the Bloom Filter a probabilistic approach, there is an inherent relationship between the number of elements in the source set, the number of elements in the Filter's index, and the number of hash functions required. This relationship is given by:
!(https://wikimedia.org/api/rest_v1/media/math/render/svg/fabc2770225ac59fe42a78f75ea89de650f0130c "Bloom Filter")

where
  * m: required number of bits (length of the bit array)
  * n: number of inserted elements
  * k: number of hash functions

This implementation sets the number of hash functions to four, so we count n (number of items to be inserted) to determine m (required number of bits). The number of bits determine how large should our index numbers should be.

If you want to change the number of hash functions used, update the constant k in the calculation to obtain the correct number of bits in the filter array.