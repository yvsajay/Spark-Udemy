import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils



def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("airports").setMaster("local[2]")
    sc = SparkContext(conf = conf)

    airports = sc.textFile("in/airports.text")
    airportsabove40 = airports.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)

    airportsNameAndLatitudeNames = airportsabove40.map(splitComma)
    airportsNameAndLatitudeNames.saveAsTextFile("out/airports_by_latitude.text")