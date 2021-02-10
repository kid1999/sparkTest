import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kid1999
 * @create 2021-02-08 21:19
 * @description TODO
 **/
public class SparkKafkaTest {
    public static void main(String[] args) throws InterruptedException {


        String brokers = "159.75.6.26:9092";
        String topic = "Test";


        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        // 每隔5秒钟，咱们的spark streaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(
                sparkConf, Durations.seconds(5));


        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        Map<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", brokers);
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "jis");
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = Arrays.asList(topic);



        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,kafkaParams)
                );

        JavaPairDStream<String, String> pairDS = stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                });

        pairDS.print();
        jssc.start();
        jssc.awaitTermination();


    }
}