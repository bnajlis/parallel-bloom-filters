""" Parallel Bloom Filter Implementation in PySpark"""
import sys
import hashlib
import math
from pyspark import SparkContext

def getopts(argv):
    """Get command line arguments and parse them into key-value pairs"""
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts

def create_spark_context():
    """ Creates default spark context"""
    sc = SparkContext("local", "ParallelBloomFilter")
    return sc

def create_bloom_filter(in_fname, out_fname):
    
    """Creates a Bloom Filter index from input filename and writes to out filename"""
    # Create Spark Context
    sc = create_spark_context()

    # Read the input file into an RDD
    in_data = sc.textFile(in_fname)

    # First we need to find out how large our Bloom Filter should be
    # This will determine the length of the indices we will get from the hash functions

    # In this implementation, the number of hash functions is preset to 4
    k = 4
    # Count the number of elements in the input
    n = in_data.count()
    # m gives how many indices we need
    m = k * n / (math.log(2))
    # Number of digits we need to take out of each hash function to get the indices

    print("*********************************************************")
    print("Bloom filter will use indices of length:", num_of_digits)
    print("*********************************************************")

    # Split the input file into multiple lines
    in_split = in_data.flatMap(lambda x: x.split())

    # Apply four different hash functions to create the Bloom filter
    # Each one of these RDDs has the indice that will be set to 1
    # to mark the existance of the input elements
    in_md5    = in_split.map(lambda x: int(hashlib.md5(x.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits))
    in_sha224 = in_split.map(lambda x: int(hashlib.sha224(x.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits))
    in_sha256 = in_split.map(lambda x: int(hashlib.sha256(x.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits))
    in_sha384 = in_split.map(lambda x: int(hashlib.sha384(x.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits))

    # Union all sets of indices, so we know the full collection of indices that have to be set to 1
    in_allhashes = in_md5.union(in_sha384).union(in_sha256).union(in_sha224)

    # As multiple hash functions can map the same input element to the same index,
    # we need to remove duplicates

    # Create a set per index, so we can use reduceByKey to remove duplicates
    in_mapped = in_allhashes.map(lambda x: (x, 1))
    # Use reduceByKey to remove index duplicates (d). Sort before saving
    in_nodupes = in_mapped.reduceByKey(lambda x, y: 1).sortByKey()
    # This is the list of indices set to 1 in the Bloom Filter
    bloom_indices = in_nodupes.map(lambda x: x[0])  

    # Save Bloom Filter indices to output file
    bloom_indices.saveAsTextFile(out_fname)

def validate_bloom_filter(infile, item, num_of_digits):
    """Checks if an item exists in a set of indices for a bloom filter"""
    # Create Spark Context
    sc = create_spark_context()

    # Get the index value to check for the four hash functions we are using
    index_md5    = int(hashlib.md5(item.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits)
    index_sha224 = int(hashlib.sha224(item.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits)
    index_sha256 = int(hashlib.sha256(item.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits)
    index_sha384 = int(hashlib.sha384(item.encode("utf-8")).hexdigest(),16) % (10 ** num_of_digits)
    
    # Read the file with the Bloom filter indices previously created
    bloom_filter_indices = sc.textFile(infile)

    # Get the indices for each of the hash functions
    is_index1 = bloom_filter_indices.filter(lambda x: int(x) == index_md5).count() > 0
    is_index2 = bloom_filter_indices.filter(lambda x: int(x) == index_sha224).count() > 0
    is_index3 = bloom_filter_indices.filter(lambda x: int(x) == index_sha256).count() > 0
    is_index4 = bloom_filter_indices.filter(lambda x: int(x) == index_sha384).count() > 0

    # Get logical AND of all index existance flags
    is_element_there = is_index1 and is_index2 and is_index3 and is_index4

    # Print message with results depending on indices 
    if is_element_there:
        print("Element ", item, "is possibly in the set of elements.")
    else:
        print("Element ", item, "is definitely not in the set of elements")



if __name__ == '__main__':
    myargs = getopts(sys.argv)
    if ('-mode' in myargs) and ('-data' in myargs) and ('-index' in myargs) :
        infile = myargs['-data']
        outfile = myargs['-index']
        create_bloom_filter(infile, outfile)
    elif ('-mode' in myargs) and ('-index' in myargs) and ('-num_digits' in myargs) and ('-validate' in myargs):
        infile = myargs['-index']
        item = myargs['-validate']
        num_of_digits = myargs['-num_digits']
        validate_bloom_filter(infile, item, num_of_digits)
    else:
        print("Parallel Bloom Filter Generator")
        print("============================")
        print()
        print("Use to generate a Bloom Filter indices file,")
        print("and validate if an item exists on a previously created index.")
        print()
        print("Usage:")
        print("To create a Bloom filter index file from an existing data file:")
        print("   spark-submit parallel-bloom-filter.py -mode create -data [data_filename] -index [index_output_filename]")
        print("Example: spark-submit parallel-bloom-filter.py -mode create -data ./data.dat -index ./bloom_filter.dat")
        print()
        print("To validate an item existing in a previously created filter:")
        print("   spark-submit parallel-bloom-filter.py -mode validate -index [index_output_filename] -item [item_to_validate]")
        print("Example: spark-submit parallel-bloom-filter.py -mode validate -index ./bloom_filter.dat -item QGTYPF&UOHY")


