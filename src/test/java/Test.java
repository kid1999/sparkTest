import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * @author kid1999
 * @create 2021-02-07 10:11
 * @description TODO
 **/
public class Test {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        Dataset<String> file = spark.read().textFile("src/main/resources/shakespeare.txt");
        file.show();
    }


}