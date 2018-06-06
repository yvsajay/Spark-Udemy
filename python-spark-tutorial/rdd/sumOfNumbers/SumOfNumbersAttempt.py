import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("primenumbers").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    prime = sc.textFile("in/prime_nums.text")
    primeinone = prime.flatMap(lambda prime: prime.split("\t"))
    primeinone_int = primeinone.map(lambda number: int(number))
    sumprime =  primeinone_int.reduce(lambda x, y: x + y)
    print("sum is :{}".format(sumprime))
    