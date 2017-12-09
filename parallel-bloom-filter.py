""" Parallel Bloom Filter Implementation in PySpark"""
import sys
import hashlib
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
    sc = create_spark_context()

    # Read the input file into an RDD
    in_data = sc.textFile(in_fname)
    # Split the input file into multiple lines
    in_split = in_data.flatMap(lambda x: x.split())

    # Apply four different hash functions to create the Bloom filter
    # Each one of these RDDs has the indice that will be set to 1
    # to mark the existance of the input elements
    in_md5    = in_split.map(lambda x: int(hashlib.md5(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
    in_sha224 = in_split.map(lambda x: int(hashlib.sha224(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
    in_sha256 = in_split.map(lambda x: int(hashlib.sha256(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
    in_sha384 = in_split.map(lambda x: int(hashlib.sha384(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))

    # Union all sets of indices, so we know the full collection of indices that have to be set to 1
    in_allhashes = in_md5.union(in_sha384).union(in_sha256).union(in_sha224)

    # As multiple hash functions can map the same input element to the same index,
    # we need to remove duplicates

    # Create a set per index, so we can use reduceByKey to remove duplicates
    in_mapped = in_allhashes.map(lambda x: (x, 1))
    # Use reduceByKey to remove index duplicates (d)
    in_nodupes = in_mapped.reduceByKey(lambda x, y: 1)
    # This is the list of indices set to 1 in the Bloom Filter
    bloom_indices = in_nodupes.map(lambda x: x[0])
    # Save Bloom Filter indices to output file
    bloom_indices.saveAsTextFile(out_fname)

def validate_bloom_filter(infile, item):
    return 0


if __name__ == '__main__':
    myargs = getopts(sys.argv)
    if '-mode' and '-data' and '-index' in myargs:
        infile = myargs['-data']
        outfile = myargs['-index']
        create_bloom_filter(infile, outfile)
    elif '-mode' and '-index' and '-validate' in myargs:
        infile = myargs['-index']
        item = myargs['-validate']
        validate_bloom_filter(infile, item)
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


