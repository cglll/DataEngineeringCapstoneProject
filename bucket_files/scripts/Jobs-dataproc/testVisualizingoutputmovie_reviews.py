from pyspark.sql import *

stagging_path="gs://debootcamptest/stagging-data/movie_sentiment/part-00000-4b7ac35b-5fd8-480c-8a74-5143bd3cae72-c000.snappy.parquet"

spark=SparkSession.builder.appName("visualizing reviews").getOrCreate()

parDF1=spark.read.parquet(stagging_path)

parDF1.show(100)
