from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_scores(rdt):
    subreddit = rdt["subreddit"]
    score = rdt["score"]
    return subreddit, (1, score)


def add_pairs(r1, r2):
    count = r1[0] + r2[0]
    score = r1[1] + r2[1]
    return count, score


def calculate_avg(rdt):
    average = rdt[1][1] / rdt[1][0]
    if (average > 0.0):
        return rdt[0], average


def get_broad_cast_score(avg_broad,comment):
    author = comment["author"]
    subreddit = comment["subreddit"]
    score = comment["score"]
    avg = avg_broad.value[subreddit]
    return score/avg, author



def get_key(kv):
    return kv[0]


def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata = text.map(json.loads).cache()
    average = commentdata.map(get_scores).reduceByKey(add_pairs).map(calculate_avg)
    avg_dict = dict(average.collect())
    avg_broad = sc.broadcast(avg_dict)  
    commentbysub = commentdata.map(lambda a: get_broad_cast_score(avg_broad,a))
    outdata = commentbysub.sortBy(get_key,ascending=False)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
