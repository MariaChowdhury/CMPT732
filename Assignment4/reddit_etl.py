from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_scores_authors(rdt):
    subreddit = rdt["subreddit"]
    score = rdt["score"]
    author = rdt["author"]
    return subreddit, score, author


def get_filtered_data(r):
    ch = 'e'
    if ch in r[0]:
        return True
    else:
        return False


def get_positive(r):
    if r[1] > 0:
        return True
    else:
        return False


def get_negative(r):
    if r[1] < 0:
        return True
    else:
        return False


def main(inputs, output):
    text = sc.textFile(inputs)
    json_value = text.map(json.loads).map(get_scores_authors).filter(get_filtered_data).cache()
    json_value.filter(get_positive).map(json.dumps).saveAsTextFile(output +
                                                                   '/positive')
    json_value.filter(get_negative).map(json.dumps).saveAsTextFile(output +
                                                                   '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
