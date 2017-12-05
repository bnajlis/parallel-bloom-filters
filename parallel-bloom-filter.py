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

def bloom_filter(in_fname, out_fname):
    """Creates a Bloom Filter index from input filename and writes to out filename"""
    sc = create_spark_context()
    in_data = sc.textFile(in_fname)
    #print(int(hashlib.md5(b"Hello World!").hexdigest(),16))

def hash_1(input):
    return int(hashlib.md5(input.encode("utf-8")).hexdigest(),16) % (10 ** 5))


if __name__ == '__main__':
    myargs = getopts(sys.argv)
    if '-create' and '-data' and '-index' in myargs:
        input = myargs['-data']
        output = myargs['-index']
        bloom_filter(input, output)
    elif '-validate' and '-index' in myargs:
        input = myargs['-index']
    else:
        print("Random sample data generator")
        print("============================")
        print("Use to generate a file with random strings of fixed length.")
        print("Usage: python3 generate-sample-data.py -samples [s] -length [l] -output [f]")
        print("Example: python3 generate-sample-data.py -samples 1000 -length 10 -output sample_data.txt")
        print("will generate 1000 strings with 10 random characters into sample_data.txt")


