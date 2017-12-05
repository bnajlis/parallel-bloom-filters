# parallel-bloom-filters
Parallelized version of Bloom Filters

There is a relation between the number of hash functions and the length of the bit array.

k: Number of hash functions
m: Length of the bit array
n: Number of elements

The length of the array for k=4 (4 hash functions) is 14,427 items in the bit array.
As our hash functions return integers larger than a reasonable size array index, we will take just a slice of the integer returned by the hash function, so that will be our new hash function (i.e. md5 + taking the last x digits of the hash digest).

Based on our calculations, with 14,427 possible indexes, we will take the last 5 positions on the digest, so our array will have 99,999 bits.