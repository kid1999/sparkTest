import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author kid1999
 * @create 2021-02-08 20:58
 * @description TODO
 **/
public class SparkSocketTest {
    public static void main(String[] args) throws InterruptedException {


        SparkConf conf = new SparkConf();
        conf.setAppName("test").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        // 读取 9092端口的socket通讯
        final JavaReceiverInputDStream<String> lines = jssc.socketTextStream("159.75.6.26", 9092);


        // 遍历每一行 分割单词 返回迭代器
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            public Iterator<String> call(String line) throws Exception {
                return (Iterator<String>) Arrays.asList(line.split(" "));
            }
        });

        // 将每个单词计数为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 将相同的单词计数标记1增加
        JavaPairDStream<String, Integer> word_count = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        word_count.print();
        jssc.awaitTermination();



    }
}