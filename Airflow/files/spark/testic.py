from pyspark.sql import SparkSession, functions as sf
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    output_transformed_dir = sys.argv[2]

    spark = SparkSession.builder.appName("prepare_initial_data").getOrCreate()

    df = spark.read.text(input_file_path)

    df.write.mode("overwrite").text(output_transformed_dir)


    spark.stop()