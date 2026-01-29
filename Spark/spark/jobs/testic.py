from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_transformed_dir = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_initial_data").getOrCreate()

    rdd = spark.sparkContext.textFile(input_file_path)

    # Sačuvaj nazad u istom formatu (text)
    rdd.saveAsTextFile(output_transformed_dir)

    spark.stop()