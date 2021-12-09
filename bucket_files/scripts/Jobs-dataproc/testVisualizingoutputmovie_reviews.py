from pyspark.sql import *

stagging_path="gs://debootcamptest/stagging-data/movie_sentiment.parquet/part-00000-b0dced4f-e405-4d08-a63c-de38da9f5509-c000.snappy.parquet"

spark=SparkSession.builder.appName("visualizing reviews").getOrCreate()

parDF1=spark.read.parquet(stagging_path)

parDF1.show(100)
