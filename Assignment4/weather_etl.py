import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

    weather = spark.read.csv(inputs, schema=observation_schema)
    filtered_qflag_data = weather.filter(weather.qflag.isNull())
    filter_station_data = filtered_qflag_data.filter(filtered_qflag_data.station.startswith('CA'))
    filter_observed_data = filter_station_data.filter(filter_station_data.observation.isin('TMAX'))
    filter_centrigrade = filter_observed_data.withColumn('tmax',filter_observed_data.value / 10)
    etl_data = filter_centrigrade.select('station','date','tmax')
    etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
