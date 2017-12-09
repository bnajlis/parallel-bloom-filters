# parallel-bloom-filters
Scalable Parallelized version of Bloom Filters
==============================================

A Bloom filter is a space-efficient probabilistic data structure, conceived by Burton Howard Bloom in 1970, that is used to test whether an element is a member of a set. False positive matches are possible, but false negatives are not – in other words, a query returns either "possibly in set" or "definitely not in set". Elements can be added to the set, but not removed (though this can be addressed with a "counting" filter); the more elements that are added to the set, the larger the probability of false positives. [Wikipedia: https://en.wikipedia.org/wiki/Bloom_filter]

There is a relationship between the number of hash functions and the length of the bit array.

k: Number of hash functions
m: Length of the bit array
n: Number of elements

The length of the array for k=4 (4 hash functions) is 144,270 items in the bit array.
As our hash functions return integers larger than a reasonable size array index, we will take just a slice of the integer returned by the hash function, so that will be our new hash function (i.e. md5 + taking the last x digits of the hash digest).

Based on our calculations, with 144,270 possible indexes, we will take the last 6 positions on the digest, so our array will have 99,999 bits.