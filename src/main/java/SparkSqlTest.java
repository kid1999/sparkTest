import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.streaming.kafka010.KafkaUtils;

/**
 * @author kid1999
 * @create 2021-02-08 20:20
 * @description TODO
 **/
public class SparkSqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "159.75.6.26:9092")
                .option("subscribe", "Test")
                .option("startingOffsets", "earliest")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

    }
}