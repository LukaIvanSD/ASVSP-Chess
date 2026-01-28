from pyspark.sql import SparkSession

def main():
    # Kreiranje SparkSession
    spark = SparkSession.builder \
        .appName("Spark Test") \
        .getOrCreate()

    # Kreiramo jednostavan dataset
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)

    # Transformacija: saberi 10 na svaki element
    rdd2 = rdd.map(lambda x: x + 10)

    # Prikaz rezultata
    print("Rezultat RDD:")
    for x in rdd2.collect():
        print(x)

    spark.stop()

if __name__ == "__main__":
    main()
