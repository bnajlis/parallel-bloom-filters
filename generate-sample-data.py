"""
Random sample data generator
============================
Use to generate a file with random strings of fixed length.
Usage: python3 generate-sample-data.py -samples [s] -length [l] -output [f]
Example: python3 generate-sample-data.py -samples 1000 -length 10 -output sample_data.txt
will generate 1000 strings with 10 random characters into sample_data.txt
"""
import sys
import random
import string
import os

def generate_samples(ns, sl, of):
    """Generate file with random sample strings of fixed length"""
    file = open(of, "w")
    valid_chars = string.ascii_uppercase + string.digits
    
    for x in range(0, ns):
        sample = ''.join(random.choice(valid_chars) for _ in range(sl))
        file.write(sample + os.linesep)
    file.close()

def getopts(argv):
    """Get command line arguments and parse them into key-value pairs"""
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts

if __name__ == '__main__':
    myargs = getopts(sys.argv)
    if '-samples' and '-length'  and '-output' in myargs:
        num_samples = int(myargs['-samples'])
        sample_length = int(myargs['-length'])
        output_filename = myargs['-output']
        generate_samples(num_samples, sample_length, output_filename)
    else:
        print("Random sample data generator")
        print("============================")
        print("Use to generate a file with random strings of fixed length.")
        print("Usage: python3 generate-sample-data.py -samples [s] -length [l] -output [f]")
        print("Example: python3 generate-sample-data.py -samples 1000 -length 10 -output sample_data.txt")
        print("will generate 1000 strings with 10 random characters into sample_data.txt")
