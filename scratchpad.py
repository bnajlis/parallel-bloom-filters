import hashlib

# Creates the bloom filter index
input = sc.textFile("data.dat")
test = input.flatMap(lambda x: x.split())
test2 = test.map(lambda x: int(hashlib.md5(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
test97 = test.map(lambda x: int(hashlib.sha224(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
test98 = test.map(lambda x: int(hashlib.sha256(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
test99 = test.map(lambda x: int(hashlib.sha384(x.encode("utf-8")).hexdigest(),16) % (10 ** 6))
test100 = test2.union(test99).union(test98).union(test97)
test3 = test100.map(lambda x: (x,1))
test4 = test3.reduceByKey(lambda x, y: 1)
test50 = test4.map(lambda x:x[0])

test4.count()

test5 = test4.sortByKey()
#Saves index to disk
test50.saveAsTextFile("test2.out")

#Reads index from disk
string_to_check = "R6DV62O2MW2EESQMQ3PNGYD33"
index1 = int(hashlib.md5(string_to_check.encode("utf-8")).hexdigest(),16) % (10 ** 6)
index2 = int(hashlib.sha224(string_to_check.encode("utf-8")).hexdigest(),16) % (10 ** 6)
index3 = int(hashlib.sha256(string_to_check.encode("utf-8")).hexdigest(),16) % (10 ** 6)
index4 = int(hashlib.sha384(string_to_check.encode("utf-8")).hexdigest(),16) % (10 ** 6)

test6 = sc.textFile("test2.out")
is_index1 = test6.filter(lambda x: int(x) == index1).count() > 0
is_index2 = test6.filter(lambda x: int(x) == index2).count() > 0
is_index3 = test6.filter(lambda x: int(x) == index3).count() > 0
is_index4 = test6.filter(lambda x: int(x) == index4).count() > 0

is_element_there = not(is_index1 and is_index2 and is_index3 and is_index4)

