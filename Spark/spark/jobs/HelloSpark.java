import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class HelloSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HelloSparkJob");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.parallelize(Arrays.asList("Luka", "Airflow", "Spark"));
        data.foreach(name -> System.out.println("Hello " + name));

        sc.close();
    }
}
